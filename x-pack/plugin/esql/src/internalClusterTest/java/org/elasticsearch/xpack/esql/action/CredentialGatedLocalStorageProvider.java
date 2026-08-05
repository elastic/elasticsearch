/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A test storage provider whose runtime contract <em>demands a decrypted plaintext credential that matches a
 * pre-declared expected value</em>. All byte reads (full and ranged) check that the provider was constructed
 * with (1) a non-{@code null} {@code expectedCredential} (the test's contract: "here's what the provider
 * should see"), and (2) a {@code credentialSeen} that is a {@link String} {@code equal} to that expected
 * value. Any deviation — a missing expected, a non-{@code String} {@code credentialSeen} (e.g. an
 * undecrypted {@code EncryptedData} carrier), or a value mismatch — makes {@link StorageObject#newStream()}
 * throw {@link IOException} with a discriminating message.
 *
 * <p>Why a value check, not just a type check: a real cloud provider rejects with 403 when the credential is
 * decrypted-but-wrong, not just when it is structurally missing. Type-only would let a future decryption bug
 * that produced the wrong plaintext slip through.
 *
 * <p>Everything else delegates to the local file system, with {@code <scheme>:///<absolute-path>} URIs
 * resolving to the file at {@code <absolute-path>}. Designed for hermetic internalClusterTests — no
 * cloud-store fixture needed — but it gives the same correctness guarantee a real provider would: no matching
 * credential, no bytes.
 */
final class CredentialGatedLocalStorageProvider implements StorageProvider {

    private final String scheme;
    private final Object credentialSeen;
    private final String expectedCredential;

    /**
     * @param scheme              the URI scheme this provider serves; the only value returned from {@link #supportedSchemes()}.
     * @param credentialSeen      the credential the factory observed at construction time.
     * @param expectedCredential  the credential the provider expects to receive. Reads succeed iff {@code credentialSeen}
     *                            is a {@link String} equal to this value. May be {@code null} to model the "provider not
     *                            configured for credentials" case, where every read denies.
     *
     *                            <p>Splitting the test cases:
     *                            <ul>
     *                              <li><b>Right credential:</b> set {@code expectedCredential} to the value the test will
     *                                  {@code PutDataSource} so the registry's decrypted plaintext matches at read time.</li>
     *                              <li><b>Wrong credential:</b> set {@code expectedCredential} to one value but
     *                                  {@code PutDataSource} a different one — the read denies with a clear "credential
     *                                  mismatch" message that an operator can diagnose.</li>
     *                              <li><b>Decryption regression:</b> if a future change ever lets an {@code EncryptedData}
     *                                  carrier reach the factory, {@code credentialSeen} will not be a {@code String} and the
     *                                  read denies with a "no decrypted plaintext credential" message.</li>
     *                            </ul>
     */
    CredentialGatedLocalStorageProvider(String scheme, Object credentialSeen, String expectedCredential) {
        this.scheme = scheme;
        this.credentialSeen = credentialSeen;
        this.expectedCredential = expectedCredential;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return new GatedLocalObject(path, credentialSeen, expectedCredential);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return new GatedLocalObject(path, credentialSeen, expectedCredential);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return new GatedLocalObject(path, credentialSeen, expectedCredential);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
        return new StorageIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public StorageEntry next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public boolean exists(StoragePath path) {
        return Files.exists(localPathOf(path));
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of(scheme);
    }

    @Override
    public void close() {}

    @SuppressForbidden(reason = "test converts a custom-scheme URI to a local Path to serve fixture bytes")
    private static Path localPathOf(StoragePath path) {
        return PathUtils.get(path.localPath());
    }

    private static final class GatedLocalObject implements StorageObject {
        private final StoragePath path;
        private final Path file;
        private final Object credentialSeen;
        private final String expectedCredential;

        GatedLocalObject(StoragePath path, Object credentialSeen, String expectedCredential) {
            this.path = path;
            this.file = localPathOf(path);
            this.credentialSeen = credentialSeen;
            this.expectedCredential = expectedCredential;
        }

        private void requireDecryptedCredential() throws IOException {
            // The test fixture must always declare an expected credential — happy paths and failure paths
            // both flow through this gate, so the test contract is "tell me what value the provider should
            // see at construction time, I'll enforce it at read time".
            if (expectedCredential == null) {
                throw new IOException("read denied: storage provider not configured with an expected credential");
            }
            if (credentialSeen instanceof String seen) {
                if (expectedCredential.equals(seen) == false) {
                    // Models a real provider rejecting a wrong password — e.g. S3 returning 403.
                    throw new IOException(
                        "read denied: credential mismatch (provider expected one value, observed a different decrypted plaintext)"
                    );
                }
                return;
            }
            throw new IOException(
                "read denied: no decrypted plaintext credential observed (saw ["
                    + (credentialSeen == null ? "null" : credentialSeen.getClass().getName())
                    + "])"
            );
        }

        @Override
        public InputStream newStream() throws IOException {
            requireDecryptedCredential();
            return Files.newInputStream(file);
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            requireDecryptedCredential();
            InputStream in = Files.newInputStream(file);
            long skipped = 0;
            while (skipped < position) {
                long n = in.skip(position - skipped);
                if (n <= 0) {
                    break;
                }
                skipped += n;
            }
            return new BoundedInputStream(in, length);
        }

        @Override
        public long length() throws IOException {
            return Files.size(file);
        }

        @Override
        public Instant lastModified() throws IOException {
            return Files.getLastModifiedTime(file).toInstant();
        }

        @Override
        public boolean exists() {
            return Files.exists(file);
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    /** Caps an InputStream to a byte length so range reads stop at the requested cell. */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;

        BoundedInputStream(InputStream delegate, long length) {
            this.delegate = delegate;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = delegate.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int n = delegate.read(b, off, toRead);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
