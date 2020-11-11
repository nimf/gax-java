/*
 * Copyright 2020 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.google.api.gax.rpc.mtls;

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.core.BetaApi;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;

/** Utilities for mutual TLS. */
@BetaApi
public class MtlsUtils {
  @VisibleForTesting
  static class DefaultMtlsProvider implements MtlsProvider {
    private static final String DEFAULT_CONTEXT_AWARE_METADATA_PATH =
        System.getProperty("user.home") + "/.secureConnect/context_aware_metadata.json";

    /** GOOGLE_API_USE_CLIENT_CERTIFICATE environment variable. */
    public static final String GOOGLE_API_USE_CLIENT_CERTIFICATE =
        "GOOGLE_API_USE_CLIENT_CERTIFICATE";

    /** GOOGLE_API_USE_MTLS_ENDPOINT environment variable. */
    public static final String GOOGLE_API_USE_MTLS_ENDPOINT = "GOOGLE_API_USE_MTLS_ENDPOINT";

    interface EnvironmentProvider {
      String getenv(String name);
    }

    static class SystemEnvironmentProvider implements EnvironmentProvider {
      @Override
      public String getenv(String name) {
        return System.getenv(name);
      }
    }

    DefaultMtlsProvider() {
      this(new SystemEnvironmentProvider(), DEFAULT_CONTEXT_AWARE_METADATA_PATH);
    }

    private EnvironmentProvider envProvider;
    private String metadataPath;

    /** Cache the key store so we don't create a new one every time getKeyStore is called. */
    private KeyStore keyStore = null;

    @VisibleForTesting
    DefaultMtlsProvider(EnvironmentProvider envProvider, String metadataPath) {
      this.envProvider = envProvider;
      this.metadataPath = metadataPath;
    }

    @Override
    public boolean useMtlsClientCertificate() {
      String useClientCertificate = envProvider.getenv(GOOGLE_API_USE_CLIENT_CERTIFICATE);
      return "true".equals(useClientCertificate);
    }

    @Override
    public String useMtlsEndpoint() {
      return envProvider.getenv(GOOGLE_API_USE_MTLS_ENDPOINT);
    }

    @Override
    public String getKeyStorePassword() {
      return "";
    }

    @Override
    public KeyStore getKeyStore() throws IOException, GeneralSecurityException {
      if (keyStore != null) {
        // return the cached key store
        return keyStore;
      }
      try {
        // Load the cert provider command from the json file.
        InputStream stream = new FileInputStream(metadataPath);
        List<String> command = extractCertificateProviderCommand(stream);

        // Run the command and timeout after 1000 milliseconds.
        Process process = new ProcessBuilder(command).start();
        int exitCode = runCertificateProviderCommand(process, 1000);
        if (exitCode != 0) {
          throw new IOException("Cert provider command failed with exit code: " + exitCode);
        }

        // Create mTLS key store with the input certificates from shell command.
        keyStore = SecurityUtils.createMtlsKeyStore(process.getInputStream());
        return keyStore;
      } catch (FileNotFoundException ignored) {
        // file doesn't exist
        return null;
      } catch (InterruptedException e) {
        throw new IOException("Interrupted executing certificate provider command", e);
      }
    }

    @VisibleForTesting
    static List<String> extractCertificateProviderCommand(InputStream contextAwareMetadata)
        throws IOException {
      JsonParser parser = new JacksonFactory().createJsonParser(contextAwareMetadata);
      ContextAwareMetadataJson json = parser.parse(ContextAwareMetadataJson.class);
      return json.getCommands();
    }

    @VisibleForTesting
    static int runCertificateProviderCommand(Process commandProcess, long timeoutMilliseconds)
        throws IOException, InterruptedException {
      long startTime = System.currentTimeMillis();
      long remainTime = timeoutMilliseconds;
      boolean terminated = false;

      do {
        try {
          // Check if process is terminated by polling the exitValue, which throws
          // IllegalThreadStateException if not terminated.
          commandProcess.exitValue();
          terminated = true;
          break;
        } catch (IllegalThreadStateException ex) {
          if (remainTime > 0) {
            Thread.sleep(Math.min(remainTime + 1, 100));
          }
        }
        remainTime = remainTime - (System.currentTimeMillis() - startTime);
      } while (remainTime > 0);

      if (!terminated) {
        commandProcess.destroy();
        throw new IOException("cert provider command timed out");
      }

      return commandProcess.exitValue();
    }
  }

  private static final MtlsProvider MTLS_PROVIDER = new DefaultMtlsProvider();

  /** Returns the default MtlsProvider instance. */
  public static MtlsProvider getDefaultMtlsProvider() {
    return MTLS_PROVIDER;
  }
}
