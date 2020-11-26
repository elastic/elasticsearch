/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.tar;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.api.GradleException;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;

public class SymbolicLinkPreservingTarIT extends GradleIntegrationTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws IOException {
        final Path realFolder = temporaryFolder.getRoot().toPath().resolve("real-folder");
        Files.createDirectory(realFolder);
        Files.createFile(realFolder.resolve("file"));
        Files.createSymbolicLink(realFolder.resolve("link-to-file"), Paths.get("./file"));
        final Path linkInFolder = temporaryFolder.getRoot().toPath().resolve("link-in-folder");
        Files.createDirectory(linkInFolder);
        Files.createSymbolicLink(linkInFolder.resolve("link-to-file"), Paths.get("../real-folder/file"));
        final Path linkToRealFolder = temporaryFolder.getRoot().toPath().resolve("link-to-real-folder");
        Files.createSymbolicLink(linkToRealFolder, Paths.get("./real-folder"));
    }

    public void testBZip2Tar() throws IOException {
        runBuild("buildBZip2Tar", true);
        assertTar(".bz2", BZip2CompressorInputStream::new, true);
    }

    public void testBZip2TarDoNotPreserveFileTimestamps() throws IOException {
        runBuild("buildBZip2Tar", false);
        assertTar(".bz2", BZip2CompressorInputStream::new, false);
    }

    public void testGZipTar() throws IOException {
        runBuild("buildGZipTar", true);
        assertTar(".gz", GzipCompressorInputStream::new, true);
    }

    public void testGZipTarDoNotPreserveFileTimestamps() throws IOException {
        runBuild("buildGZipTar", false);
        assertTar(".gz", GzipCompressorInputStream::new, false);
    }

    public void testTar() throws IOException {
        runBuild("buildTar", true);
        assertTar("", fis -> fis, true);
    }

    public void testTarDoNotPreserveFileTimestamps() throws IOException {
        runBuild("buildTar", false);
        assertTar("", fis -> fis, false);
    }

    interface FileInputStreamWrapper {
        InputStream apply(FileInputStream fis) throws IOException;
    }

    private void assertTar(final String extension, final FileInputStreamWrapper wrapper, boolean preserveFileTimestamps)
        throws IOException {
        try (TarArchiveInputStream tar = new TarArchiveInputStream(wrapper.apply(new FileInputStream(getOutputFile(extension))))) {
            TarArchiveEntry entry = tar.getNextTarEntry();
            boolean realFolderEntry = false;
            boolean fileEntry = false;
            boolean linkToFileEntry = false;
            boolean linkInFolderEntry = false;
            boolean linkInFolderLinkToFileEntry = false;
            boolean linkToRealFolderEntry = false;
            while (entry != null) {
                if (entry.getName().equals("real-folder/")) {
                    assertTrue(entry.isDirectory());
                    realFolderEntry = true;
                } else if (entry.getName().equals("real-folder/file")) {
                    assertTrue(entry.isFile());
                    fileEntry = true;
                } else if (entry.getName().equals("real-folder/link-to-file")) {
                    assertTrue(entry.isSymbolicLink());
                    assertThat(entry.getLinkName(), anyOf(equalTo("./file"), equalTo(".\\file")));
                    linkToFileEntry = true;
                } else if (entry.getName().equals("link-in-folder/")) {
                    assertTrue(entry.isDirectory());
                    linkInFolderEntry = true;
                } else if (entry.getName().equals("link-in-folder/link-to-file")) {
                    assertTrue(entry.isSymbolicLink());
                    assertThat(entry.getLinkName(), anyOf(equalTo("../real-folder/file"), equalTo("..\\real-folder\\file")));
                    linkInFolderLinkToFileEntry = true;
                } else if (entry.getName().equals("link-to-real-folder")) {
                    assertTrue(entry.isSymbolicLink());
                    assertThat(entry.getLinkName(), anyOf(equalTo("./real-folder"), equalTo(".\\real-folder")));
                    linkToRealFolderEntry = true;
                } else {
                    throw new GradleException("unexpected entry [" + entry.getName() + "]");
                }
                if (preserveFileTimestamps) {
                    assertTrue(entry.getModTime().getTime() > 0);
                } else {
                    assertThat(entry.getModTime().getTime(), equalTo(0L));
                }
                entry = tar.getNextTarEntry();
            }
            assertTrue(realFolderEntry);
            assertTrue(fileEntry);
            assertTrue(linkToFileEntry);
            assertTrue(linkInFolderEntry);
            assertTrue(linkInFolderLinkToFileEntry);
            assertTrue(linkToRealFolderEntry);
        }
    }

    private void runBuild(final String task, final boolean preserveFileTimestamps) {
        final GradleRunner runner = GradleRunner.create()
            .withProjectDir(getProjectDir())
            .withArguments(
                task,
                "-Dtests.symbolic_link_preserving_tar_source=" + temporaryFolder.getRoot().toString(),
                "-Dtests.symbolic_link_preserving_tar_preserve_file_timestamps=" + preserveFileTimestamps,
                "-i"
            )
            .withPluginClasspath();

        runner.build();
    }

    private File getProjectDir() {
        return getProjectDir("symbolic-link-preserving-tar");
    }

    private File getOutputFile(final String extension) {
        return getProjectDir().toPath().resolve("build/distributions/symbolic-link-preserving-tar.tar" + extension).toFile();
    }

}
