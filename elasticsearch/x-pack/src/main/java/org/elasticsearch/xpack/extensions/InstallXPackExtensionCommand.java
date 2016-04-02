/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.util.IOUtils;

import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.env.Environment;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.xpack.XPackPlugin.resolveXPackExtensionsFile;
import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the extension cli to install an extension into x-pack.
 *
 * The install command takes a URL to an extension zip.
 *
 * Extensions are packaged as zip files. Each packaged extension must contain an
 * extension properties file. See {@link XPackExtensionInfo}.
 * <p>
 * The installation process first extracts the extensions files into a temporary
 * directory in order to verify the extension satisfies the following requirements:
 * <ul>
 *     <li>The property file exists and contains valid metadata. See {@link XPackExtensionInfo#readFromProperties(Path)}</li>
 *     <li>Jar hell does not exist, either between the extension's own jars or with the parent classloader (elasticsearch + x-pack)</li>
 * </ul>
 */
class InstallXPackExtensionCommand extends Command {

    private final Environment env;
    private final OptionSpec<Void> batchOption;
    private final OptionSpec<String> arguments;

    InstallXPackExtensionCommand(Environment env) {
        super("Install a plugin");
        this.env = env;
        this.batchOption = parser.acceptsAll(Arrays.asList("b", "batch"),
                "Enable batch mode explicitly, automatic confirmation of security permission");
        this.arguments = parser.nonOptions("plugin id");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        // TODO: in jopt-simple 5.0 we can enforce a min/max number of positional args
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserError(ExitCodes.USAGE, "Must supply a single extension id argument");
        }
        String extensionURL = args.get(0);
        boolean isBatch = options.has(batchOption) || System.console() == null;
        execute(terminal, extensionURL, isBatch);
    }


    // pkg private for testing
    void execute(Terminal terminal, String extensionId, boolean isBatch) throws Exception {
        if (Files.exists(resolveXPackExtensionsFile(env)) == false) {
            terminal.println("xpack extensions directory [" + resolveXPackExtensionsFile(env) + "] does not exist. Creating...");
            Files.createDirectories(resolveXPackExtensionsFile(env));
        }

        Path extensionZip = download(terminal, extensionId, env.tmpFile());
        Path extractedZip = unzip(extensionZip, resolveXPackExtensionsFile(env));
        install(terminal, extractedZip, env);
    }

    /** Downloads the extension and returns the file it was downloaded to. */
    private Path download(Terminal terminal, String extensionURL, Path tmpDir) throws Exception {
        terminal.println("-> Downloading " + URLDecoder.decode(extensionURL, "UTF-8"));
        URL url = new URL(extensionURL);
        Path zip = Files.createTempFile(tmpDir, null, ".zip");
        try (InputStream in = url.openStream()) {
            // must overwrite since creating the temp file above actually created the file
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }
        return zip;
    }

    private Path unzip(Path zip, Path extensionDir) throws IOException, UserError {
        // unzip extension to a staging temp dir
        Path target = Files.createTempDirectory(extensionDir, ".installing-");
        Files.createDirectories(target);

        // TODO: we should wrap this in a try/catch and try deleting the target dir on failure?
        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                Path targetFile = target.resolve(entry.getName());
                // TODO: handle name being an absolute path

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                Files.createDirectories(targetFile.getParent());
                if (entry.isDirectory() == false) {
                    try (OutputStream out = Files.newOutputStream(targetFile)) {
                        int len;
                        while((len = zipInput.read(buffer)) >= 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipInput.closeEntry();
            }
        }
        Files.delete(zip);
        return target;
    }

    /** Load information about the extension, and verify it can be installed with no errors. */
    private XPackExtensionInfo verify(Terminal terminal, Path extensionRoot, Environment env) throws Exception {
        // read and validate the extension descriptor
        XPackExtensionInfo info = XPackExtensionInfo.readFromProperties(extensionRoot);
        terminal.println(VERBOSE, info.toString());

        // check for jar hell before any copying
        jarHellCheck(extensionRoot);
        return info;
    }

    /** check a candidate extension for jar hell before installing it */
    private void jarHellCheck(Path candidate) throws Exception {
        // create list of current jars in classpath
        // including the x-pack jars (see $ES_CLASSPATH in bin/extension script)
        final List<URL> jars = new ArrayList<>();
        jars.addAll(Arrays.asList(JarHell.parseClassPath()));

        // add extension jars to the list
        Path extensionJars[] = FileSystemUtils.files(candidate, "*.jar");
        for (Path jar : extensionJars) {
            jars.add(jar.toUri().toURL());
        }
        // TODO: no jars should be an error
        // TODO: verify the classname exists in one of the jars!

        // check combined (current classpath + new jars to-be-added)
        JarHell.checkJarHell(jars.toArray(new URL[jars.size()]));
    }

    /**
     * Installs the extension from {@code tmpRoot} into the extensions dir.
     */
    private void install(Terminal terminal, Path tmpRoot, Environment env) throws Exception {
        List<Path> deleteOnFailure = new ArrayList<>();
        deleteOnFailure.add(tmpRoot);
        try {
            XPackExtensionInfo info = verify(terminal, tmpRoot, env);
            final Path destination = resolveXPackExtensionsFile(env).resolve(info.getName());
            if (Files.exists(destination)) {
                throw new UserError(ExitCodes.USAGE,
                        "extension directory " + destination.toAbsolutePath() +
                                " already exists. To update the extension, uninstall it first using 'remove " +
                                info.getName() + "' command");
            }
            Files.move(tmpRoot, destination, StandardCopyOption.ATOMIC_MOVE);
            terminal.println("-> Installed " + info.getName());
        } catch (Exception installProblem) {
            try {
                IOUtils.rm(deleteOnFailure.toArray(new Path[0]));
            } catch (IOException exceptionWhileRemovingFiles) {
                installProblem.addSuppressed(exceptionWhileRemovingFiles);
            }
            throw installProblem;
        }
    }
}
