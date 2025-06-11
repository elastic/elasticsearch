/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.ingest.geoip.MaxmindIpDataLookups.CacheableCountryResponse;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.equalTo;

public class DatabaseNodeServiceIT extends AbstractGeoIpIT {
    /*
     * This test makes sure that if we index an ordinary mmdb file into the .geoip_databases index, it is correctly handled upon retrieval.
     */
    public void testNonGzippedDatabase() throws Exception {
        String databaseType = "GeoLite2-Country";
        String databaseFileName = databaseType + ".mmdb";
        // making the database name unique so we know we're not using another one:
        String databaseName = randomAlphaOfLength(20) + "-" + databaseFileName;
        byte[] mmdbBytes = getBytesForFile(databaseFileName);
        final DatabaseNodeService databaseNodeService = internalCluster().getInstance(DatabaseNodeService.class);
        assertNull(databaseNodeService.getDatabase(databaseName));
        int numChunks = indexData(databaseName, mmdbBytes);
        /*
         * If DatabaseNodeService::checkDatabases runs it will sometimes (rarely) remove the database we are using in this test while we
         * are trying to assert things about it. So if it does then we 'just' try again.
         */
        assertBusy(() -> {
            retrieveDatabase(databaseNodeService, databaseName, mmdbBytes, numChunks);
            assertNotNull(databaseNodeService.getDatabase(databaseName));
            assertValidDatabase(databaseNodeService, databaseName, databaseType);
        });
    }

    /*
     * This test makes sure that if we index a gzipped tar file wrapping an mmdb file into the .geoip_databases index, it is correctly
     * handled upon retrieval.
     */
    public void testGzippedDatabase() throws Exception {
        String databaseType = "GeoLite2-Country";
        String databaseFileName = databaseType + ".mmdb";
        // making the database name unique so we know we're not using another one:
        String databaseName = randomAlphaOfLength(20) + "-" + databaseFileName;
        byte[] mmdbBytes = getBytesForFile(databaseFileName);
        byte[] gzipBytes = gzipFileBytes(databaseName, mmdbBytes);
        final DatabaseNodeService databaseNodeService = internalCluster().getInstance(DatabaseNodeService.class);
        assertNull(databaseNodeService.getDatabase(databaseName));
        int numChunks = indexData(databaseName, gzipBytes);
        /*
         * If DatabaseNodeService::checkDatabases runs it will sometimes (rarely) remove the database we are using in this test while we
         * are trying to assert things about it. So if it does then we 'just' try again.
         */
        assertBusy(() -> {
            retrieveDatabase(databaseNodeService, databaseName, gzipBytes, numChunks);
            assertNotNull(databaseNodeService.getDatabase(databaseName));
            assertValidDatabase(databaseNodeService, databaseName, databaseType);
        });
    }

    /*
     * This makes sure that the database is generally usable
     */
    private void assertValidDatabase(DatabaseNodeService databaseNodeService, String databaseFileName, String databaseType)
        throws IOException {
        IpDatabase database = databaseNodeService.getDatabase(databaseFileName);
        assertNotNull(database);
        assertThat(database.getDatabaseType(), equalTo(databaseType));
        CacheableCountryResponse countryResponse = database.getResponse("89.160.20.128", GeoIpTestUtils::getCountry).result();
        assertNotNull(countryResponse);
        assertThat(countryResponse.countryName(), equalTo("Sweden"));
    }

    /*
     * This has the databaseNodeService retrieve the database from the .geoip_databases index, making the database ready for use when
     * databaseNodeService.getDatabase(databaseFileName) is called.
     */
    private void retrieveDatabase(DatabaseNodeService databaseNodeService, String databaseFileName, byte[] expectedBytes, int numChunks)
        throws IOException {
        GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(1, 0, numChunks - 1, getMd5(expectedBytes), 1);
        databaseNodeService.retrieveAndUpdateDatabase(databaseFileName, metadata);
    }

    private String getMd5(byte[] bytes) {
        MessageDigest md = MessageDigests.md5();
        md.update(bytes);
        return MessageDigests.toHexString(md.digest());
    }

    private byte[] gzipFileBytes(String databaseName, byte[] mmdbBytes) throws IOException {
        final byte[] EMPTY_BUF = new byte[512];
        Path mmdbFile = createTempFile();
        Files.copy(new ByteArrayInputStream(mmdbBytes), mmdbFile, StandardCopyOption.REPLACE_EXISTING);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStream gos = new GZIPOutputStream(new BufferedOutputStream(baos))) {
            long size = Files.size(mmdbFile);
            gos.write(createTarHeader(databaseName, size));
            Files.copy(mmdbFile, gos);
            if (size % 512 != 0) {
                gos.write(EMPTY_BUF, 0, (int) (512 - (size % 512)));
            }
            gos.write(EMPTY_BUF);
            gos.write(EMPTY_BUF);
        }
        return baos.toByteArray();
    }

    private static byte[] createTarHeader(String name, long size) {
        byte[] buf = new byte[512];
        byte[] sizeBytes = String.format(Locale.ROOT, "%1$012o", size).getBytes(StandardCharsets.UTF_8);
        byte[] nameBytes = name.substring(Math.max(0, name.length() - 100)).getBytes(StandardCharsets.US_ASCII);
        byte[] id = "0001750".getBytes(StandardCharsets.UTF_8);
        byte[] permission = "000644 ".getBytes(StandardCharsets.UTF_8);
        byte[] time = String.format(Locale.ROOT, "%1$012o", System.currentTimeMillis() / 1000).getBytes(StandardCharsets.UTF_8);
        System.arraycopy(nameBytes, 0, buf, 0, nameBytes.length);
        System.arraycopy(permission, 0, buf, 100, 7);
        System.arraycopy(id, 0, buf, 108, 7);
        System.arraycopy(id, 0, buf, 116, 7);
        System.arraycopy(sizeBytes, 0, buf, 124, 12);
        System.arraycopy(time, 0, buf, 136, 12);
        int checksum = 256;
        for (byte b : buf) {
            checksum += b & 0xFF;
        }
        byte[] checksumBytes = String.format(Locale.ROOT, "%1$07o", checksum).getBytes(StandardCharsets.UTF_8);
        System.arraycopy(checksumBytes, 0, buf, 148, 7);
        return buf;
    }

    /*
     * Finds the given databaseFileName on the classpath, and returns its bytes.
     */
    private static byte[] getBytesForFile(String databaseFileName) throws IOException {
        try (InputStream is = DatabaseNodeServiceIT.class.getResourceAsStream("/" + databaseFileName)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + databaseFileName + "] not found in classpath");
            }
            try (BufferedInputStream bis = new BufferedInputStream(is)) {
                return bis.readAllBytes();
            }
        }
    }

    /*
     * This indexes data into the .geoip_databases index in a random number of chunks.
     */
    private static int indexData(String databaseFileName, byte[] content) throws IOException {
        List<byte[]> chunks = chunkBytes(content, randomIntBetween(1, 100));
        indexChunks(databaseFileName, chunks);
        return chunks.size();
    }

    /*
     * This turns the given content bytes into the given number of chunks.
     */
    private static List<byte[]> chunkBytes(byte[] content, int chunks) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(content);
        byteArrayOutputStream.close();

        byte[] all = byteArrayOutputStream.toByteArray();
        int chunkSize = Math.max(1, all.length / chunks);
        List<byte[]> data = new ArrayList<>();

        for (int from = 0; from < all.length;) {
            int to = from + chunkSize;
            if (to > all.length) {
                to = all.length;
            }
            data.add(Arrays.copyOfRange(all, from, to));
            from = to;
        }

        while (data.size() > chunks) {
            byte[] last = data.removeLast();
            byte[] secondLast = data.removeLast();
            byte[] merged = new byte[secondLast.length + last.length];
            System.arraycopy(secondLast, 0, merged, 0, secondLast.length);
            System.arraycopy(last, 0, merged, secondLast.length, last.length);
            data.add(merged);
        }
        return data;
    }

    /*
     * This writes the given chunks into the .geoip_databases index.
     */
    private static void indexChunks(String name, List<byte[]> chunks) {
        int chunk = 0;
        for (byte[] buf : chunks) {
            IndexRequest indexRequest = new IndexRequest(GeoIpDownloader.DATABASES_INDEX).id(name + "_" + chunk + "_" + 1)
                .create(true)
                .source(XContentType.SMILE, "name", name, "chunk", chunk, "data", buf);
            client().index(indexRequest).actionGet();
            chunk++;
        }
        FlushRequest flushRequest = new FlushRequest(GeoIpDownloader.DATABASES_INDEX);
        client().admin().indices().flush(flushRequest).actionGet();
        // Ensure that the chunk documents are visible:
        RefreshRequest refreshRequest = new RefreshRequest(GeoIpDownloader.DATABASES_INDEX);
        client().admin().indices().refresh(refreshRequest).actionGet();
    }
}
