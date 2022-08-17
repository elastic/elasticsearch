/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.api.GradleException
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.function.Function

class SymbolicLinkPreservingTarFuncTest extends AbstractGradleFuncTest {

    def setup() {
        def archiveSourceRoot = testProjectDir.getRoot().toPath().resolve("archiveRoot")
        Files.createDirectory(archiveSourceRoot)
        final Path realFolder = archiveSourceRoot.resolve("real-folder")
        Files.createDirectory(realFolder);
        Files.createFile(realFolder.resolve("file"));
        Files.createSymbolicLink(realFolder.resolve("link-to-file"), Paths.get("./file"));
        final Path linkInFolder = archiveSourceRoot.resolve("link-in-folder");
        Files.createDirectory(linkInFolder);
        Files.createSymbolicLink(linkInFolder.resolve("link-to-file"), Paths.get("../real-folder/file"));
        final Path linkToRealFolder = archiveSourceRoot.resolve("link-to-real-folder");
        Files.createSymbolicLink(linkToRealFolder, Paths.get("./real-folder"));

        buildFile << """
import org.elasticsearch.gradle.internal.SymbolicLinkPreservingTar

plugins {
  id 'base'
  id 'distribution'
  // we need to add one of our plugins to get things on this classpath
  id 'elasticsearch.global-build-info' apply false
}
"""
    }

    @Unroll
    def "build BZip2 tar with preserverTimestamp #preserverTimestamp"() {
        buildFile << """
tasks.register("buildBZip2Tar", SymbolicLinkPreservingTar) { SymbolicLinkPreservingTar tar ->
  tar.archiveExtension = 'tar.bz2'
  tar.compression = Compression.BZIP2
  tar.preserveFileTimestamps = ${preserverTimestamp}
  from fileTree("archiveRoot")
}
"""
        when:
        gradleRunner("buildBZip2Tar", "-s").build();

        then:
        assertTar(file("build/distributions/hello-world.tar.bz2"), BZip2CompressorInputStream::new, preserverTimestamp)
        where:
        preserverTimestamp << [true, false]
    }

    @Unroll
    def "build GZipTar tar with preserverTimestamp #preserverTimestamp"() {
        buildFile << """
tasks.register("buildGZipTar", SymbolicLinkPreservingTar) { SymbolicLinkPreservingTar tar ->
  tar.archiveExtension = 'tar.gz'
  tar.compression = Compression.GZIP
  tar.preserveFileTimestamps = ${preserverTimestamp}
  from fileTree("archiveRoot")
}
"""
        when:
        gradleRunner("buildGZipTar").build();

        then:
        assertTar(file("build/distributions/hello-world.tar.gz"), GzipCompressorInputStream::new, preserverTimestamp)
        where:
        preserverTimestamp << [true, false]
    }

    @Unroll
    def "build tar with preserverTimestamp #preserverTimestamp"() {
        buildFile << """
tasks.register("buildTar", SymbolicLinkPreservingTar) { SymbolicLinkPreservingTar tar ->
  tar.archiveExtension = 'tar'
  tar.preserveFileTimestamps = ${preserverTimestamp}
  from fileTree("archiveRoot")
}
"""
        when:
        gradleRunner("buildTar").build();

        then:
        assertTar(file("build/distributions/hello-world.tar"), fis -> fis, preserverTimestamp)
        where:
        preserverTimestamp << [true, false]
    }

    private boolean assertTar(final File archive, final Function<? super FileInputStream, ? extends InputStream> wrapper, boolean preserveFileTimestamps)
            throws IOException {
        try (TarArchiveInputStream tar = new TarArchiveInputStream(wrapper.apply(new FileInputStream(archive)))) {
            TarArchiveEntry entry = tar.getNextTarEntry();
            boolean realFolderEntry = false;
            boolean fileEntry = false;
            boolean linkToFileEntry = false;
            boolean linkInFolderEntry = false;
            boolean linkInFolderLinkToFileEntry = false;
            boolean linkToRealFolderEntry = false;
            while (entry != null) {
                if (entry.getName().equals("real-folder/")) {
                    assert entry.isDirectory()
                    realFolderEntry = true
                } else if (entry.getName().equals("real-folder/file")) {
                    assert entry.isFile()
                    fileEntry = true
                } else if (entry.getName().equals("real-folder/link-to-file")) {
                    assert entry.isSymbolicLink()
                    assert normalized(entry.getLinkName()) == "./file"
                    linkToFileEntry = true
                } else if (entry.getName().equals("link-in-folder/")) {
                    assert entry.isDirectory()
                    linkInFolderEntry = true
                } else if (entry.getName().equals("link-in-folder/link-to-file")) {
                    assert entry.isSymbolicLink()
                    assert normalized(entry.getLinkName()) == "../real-folder/file"
                    linkInFolderLinkToFileEntry = true
                } else if (entry.getName().equals("link-to-real-folder")) {
                    assert entry.isSymbolicLink()
                    assert normalized(entry.getLinkName()) == "./real-folder"
                    linkToRealFolderEntry = true
                } else {
                    throw new GradleException("unexpected entry [" + entry.getName() + "]")
                }
                if (preserveFileTimestamps) {
                    assert entry.getModTime().getTime() > 0
                } else {
                    assert entry.getModTime().getTime() == 0L
                }
                entry = tar.getNextTarEntry()
            }
            assert realFolderEntry
            assert fileEntry
            assert linkToFileEntry
            assert linkInFolderEntry
            assert linkInFolderLinkToFileEntry
            assert linkToRealFolderEntry
        }
        true
    }

}
