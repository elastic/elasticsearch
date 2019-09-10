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

import static org.hamcrest.CoreMatchers.equalTo;

public class SymbolicLinkPreservingTarIT extends GradleIntegrationTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws IOException {
        final Path realFolder = temporaryFolder.getRoot().toPath().resolve("real-folder");
        Files.createDirectory(realFolder);
        Files.createFile(realFolder.resolve("file"));
        final Path linkFolder = temporaryFolder.getRoot().toPath().resolve("link-folder");
        Files.createDirectory(linkFolder);
        Files.createSymbolicLink(linkFolder.resolve("link"), Paths.get("../real-folder/file"));
    }

    public void testBZip2Tar() throws IOException {
        runBuild("buildBZip2Tar");
        assertTar(".bz2", BZip2CompressorInputStream::new);
    }

    public void testGZipTar() throws IOException {
        runBuild("buildGZipTar");
        assertTar(".gz", GzipCompressorInputStream::new);
    }

    public void testTar() throws IOException {
        runBuild("buildTar");
        assertTar("", fis -> fis);
    }

    interface FileInputStreamWrapper {
        InputStream apply(FileInputStream fis) throws IOException;
    }

    private void assertTar(final String extension, final FileInputStreamWrapper wrapper) throws IOException {
        try (TarArchiveInputStream tar = new TarArchiveInputStream(wrapper.apply(new FileInputStream(getOutputFile(extension))))) {
            TarArchiveEntry entry = tar.getNextTarEntry();
            boolean realFolderEntry = false;
            boolean fileEntry = false;
            boolean linkFolderEntry = false;
            boolean linkEntry = false;
            while (entry != null) {
                if (entry.getName().equals("real-folder/")) {
                    assertTrue(entry.isDirectory());
                    realFolderEntry = true;
                } else if (entry.getName().equals("real-folder/file")) {
                    assertTrue(entry.isFile());
                    fileEntry = true;
                } else if (entry.getName().equals("link-folder/")) {
                    assertTrue(entry.isDirectory());
                    linkFolderEntry = true;
                } else if (entry.getName().equals("link-folder/link")) {
                    assertTrue(entry.isSymbolicLink());
                    assertThat(entry.getLinkName(), equalTo("../real-folder/file"));
                    linkEntry = true;
                } else {
                    throw new GradleException("unexpected entry [" + entry.getName() + "]");
                }
                entry = tar.getNextTarEntry();
            }
            assertTrue(realFolderEntry);
            assertTrue(fileEntry);
            assertTrue(linkFolderEntry);
            assertTrue(linkEntry);
        }
    }

    private void runBuild(final String task) {
        final GradleRunner runner = GradleRunner.create().withProjectDir(getProjectDir())
            .withArguments(
                task,
                "-Dtests.symbolic_link_preserving_tar_source=" + temporaryFolder.getRoot().toString(),
                "-i")
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
