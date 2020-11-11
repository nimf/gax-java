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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MtlsUtilsTest {
  static class TestEnvironmentProvider
      implements MtlsUtils.DefaultMtlsProvider.EnvironmentProvider {
    private final String value;

    TestEnvironmentProvider(String value) {
      this.value = value;
    }

    @Override
    public String getenv(String name) {
      return value;
    }
  }

  @Test
  public void testUseMtlsClientCertificateEmpty() {
    MtlsProvider mtlsProvider =
        new MtlsUtils.DefaultMtlsProvider(new TestEnvironmentProvider(""), "/path/to/missing/file");
    assertFalse(mtlsProvider.useMtlsClientCertificate());
  }

  @Test
  public void testUseMtlsClientCertificateNull() {
    MtlsProvider mtlsProvider =
        new MtlsUtils.DefaultMtlsProvider(
            new TestEnvironmentProvider(null), "/path/to/missing/file");
    assertFalse(mtlsProvider.useMtlsClientCertificate());
  }

  @Test
  public void testUseMtlsClientCertificateTrue() {
    MtlsProvider mtlsProvider =
        new MtlsUtils.DefaultMtlsProvider(
            new TestEnvironmentProvider("true"), "/path/to/missing/file");
    assertTrue(mtlsProvider.useMtlsClientCertificate());
  }

  @Test
  public void testLoadDefaultKeyStoreMissingFile()
      throws InterruptedException, GeneralSecurityException, IOException {
    MtlsProvider mtlsProvider =
        new MtlsUtils.DefaultMtlsProvider(
            new TestEnvironmentProvider("true"), "/path/to/missing/file");
    KeyStore keyStore = mtlsProvider.getKeyStore();
    assertNull(keyStore);
  }

  @Test
  public void testLoadDefaultKeyStore()
      throws InterruptedException, GeneralSecurityException, IOException {
    MtlsProvider mtlsProvider =
        new MtlsUtils.DefaultMtlsProvider(
            new TestEnvironmentProvider("true"),
            "src/test/resources/com/google/api/gax/rpc/mtls/mtls_context_aware_metadata.json");
    KeyStore keyStore = mtlsProvider.getKeyStore();
    assertNotNull(keyStore);
  }

  @Test
  public void testLoadDefaultKeyStoreBadCertificate()
      throws InterruptedException, GeneralSecurityException, IOException {
    MtlsProvider mtlsProvider =
        new MtlsUtils.DefaultMtlsProvider(
            new TestEnvironmentProvider("true"),
            "src/test/resources/com/google/api/gax/rpc/mtls/mtls_context_aware_metadata_bad_command.json");
    try {
      mtlsProvider.getKeyStore();
      fail("should throw and exception");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "expected to fail with certificate is missing",
          e.getMessage().contains("certificate is missing"));
    }
  }

  @Test
  public void testExtractCertificateProviderCommand() throws IOException {
    InputStream inputStream =
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("com/google/api/gax/rpc/mtls/mtls_context_aware_metadata.json");
    List<String> command =
        MtlsUtils.DefaultMtlsProvider.extractCertificateProviderCommand(inputStream);
    assertEquals(2, command.size());
    assertEquals("cat", command.get(0));
    assertEquals(
        "src/test/resources/com/google/api/gax/rpc/mtls/mtlsCertAndKey.pem", command.get(1));
  }

  static class TestCertProviderCommandProcess extends Process {
    private boolean runForever;
    private int exitValue;

    public TestCertProviderCommandProcess(int exitValue, boolean runForever) {
      this.runForever = runForever;
      this.exitValue = exitValue;
    }

    @Override
    public OutputStream getOutputStream() {
      return null;
    }

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public InputStream getErrorStream() {
      return null;
    }

    @Override
    public int waitFor() throws InterruptedException {
      return 0;
    }

    @Override
    public int exitValue() {
      if (runForever) {
        throw new IllegalThreadStateException();
      }
      return exitValue;
    }

    @Override
    public void destroy() {}
  }

  @Test
  public void testRunCertificateProviderCommandSuccess() throws IOException, InterruptedException {
    Process certCommandProcess = new TestCertProviderCommandProcess(0, false);
    int exitValue =
        MtlsUtils.DefaultMtlsProvider.runCertificateProviderCommand(certCommandProcess, 100);
    assertEquals(0, exitValue);
  }

  @Test
  public void testRunCertificateProviderCommandTimeout() throws InterruptedException {
    Process certCommandProcess = new TestCertProviderCommandProcess(0, true);
    try {
      MtlsUtils.DefaultMtlsProvider.runCertificateProviderCommand(certCommandProcess, 100);
      fail("should throw and exception");
    } catch (IOException e) {
      assertTrue(
          "expected to fail with timeout",
          e.getMessage().contains("cert provider command timed out"));
    }
  }
}
