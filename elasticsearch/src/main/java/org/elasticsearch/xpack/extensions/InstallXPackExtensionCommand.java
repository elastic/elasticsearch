/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import java.security.Policy;
import java.security.PermissionCollection;
import java.security.Permission;
import java.security.NoSuchAlgorithmException;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.AccessController;
import java.security.UnresolvedPermission;
import java.security.URIParameter;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.xpack.XPackPlugin.resolveXPackExtensionsFile;

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
 *     <li>If the extension contains extra security permissions, the policy file is validated</li>
 * </ul>
 */
final class InstallXPackExtensionCommand extends EnvironmentAwareCommand {

    private final OptionSpec<Void> batchOption;
    private final OptionSpec<String> arguments;

    InstallXPackExtensionCommand() {
        super("Install an extension");
        this.batchOption = parser.acceptsAll(Arrays.asList("b", "batch"),
                "Enable batch mode explicitly, automatic confirmation of security permission");
        this.arguments = parser.nonOptions("extension id");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        // TODO: in jopt-simple 5.0 we can enforce a min/max number of positional args
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserException(ExitCodes.USAGE, "Must supply a single extension id argument");
        }
        String extensionURL = args.get(0);
        boolean isBatch = options.has(batchOption) || System.console() == null;
        execute(terminal, extensionURL, isBatch, env);
    }


    // pkg private for testing
    void execute(Terminal terminal, String extensionId, boolean isBatch, Environment env) throws Exception {
        if (Files.exists(resolveXPackExtensionsFile(env)) == false) {
            terminal.println("xpack extensions directory [" + resolveXPackExtensionsFile(env) + "] does not exist. Creating...");
            Files.createDirectories(resolveXPackExtensionsFile(env));
        }

        Path extensionZip = download(terminal, extensionId, env.tmpFile());
        Path extractedZip = unzip(extensionZip, resolveXPackExtensionsFile(env));
        install(terminal, extractedZip, env, isBatch);
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

    private Path unzip(Path zip, Path extensionDir) throws IOException, UserException {
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
    private XPackExtensionInfo verify(Terminal terminal, Path extensionRoot, Environment env, boolean isBatch) throws Exception {
        // read and validate the extension descriptor
        XPackExtensionInfo info = XPackExtensionInfo.readFromProperties(extensionRoot);
        terminal.println(VERBOSE, info.toString());

        // check for jar hell before any copying
        jarHellCheck(extensionRoot);

        // read optional security policy (extra permissions)
        // if it exists, confirm or warn the user
        Path policy = extensionRoot.resolve(XPackExtensionInfo.XPACK_EXTENSION_POLICY);
        if (Files.exists(policy)) {
            readPolicy(policy, terminal, env, isBatch);
        }

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
    private void install(Terminal terminal, Path tmpRoot, Environment env, boolean isBatch) throws Exception {
        List<Path> deleteOnFailure = new ArrayList<>();
        deleteOnFailure.add(tmpRoot);
        try {
            XPackExtensionInfo info = verify(terminal, tmpRoot, env, isBatch);
            final Path destination = resolveXPackExtensionsFile(env).resolve(info.getName());
            if (Files.exists(destination)) {
                throw new UserException(ExitCodes.USAGE,
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

    /** Format permission type, name, and actions into a string */
    static String formatPermission(Permission permission) {
        StringBuilder sb = new StringBuilder();

        String clazz = null;
        if (permission instanceof UnresolvedPermission) {
            clazz = ((UnresolvedPermission) permission).getUnresolvedType();
        } else {
            clazz = permission.getClass().getName();
        }
        sb.append(clazz);

        String name = null;
        if (permission instanceof UnresolvedPermission) {
            name = ((UnresolvedPermission) permission).getUnresolvedName();
        } else {
            name = permission.getName();
        }
        if (name != null && name.length() > 0) {
            sb.append(' ');
            sb.append(name);
        }

        String actions = null;
        if (permission instanceof UnresolvedPermission) {
            actions = ((UnresolvedPermission) permission).getUnresolvedActions();
        } else {
            actions = permission.getActions();
        }
        if (actions != null && actions.length() > 0) {
            sb.append(' ');
            sb.append(actions);
        }
        return sb.toString();
    }

    /**
     * Parses extension policy into a set of permissions
     */
    static PermissionCollection parsePermissions(Path file, Path tmpDir) throws IOException {
        // create a zero byte file for "comparison"
        // this is necessary because the default policy impl automatically grants two permissions:
        // 1. permission to exitVM (which we ignore)
        // 2. read permission to the code itself (e.g. jar file of the code)

        Path emptyPolicyFile = Files.createTempFile(tmpDir, "empty", "tmp");
        final Policy emptyPolicy;
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        emptyPolicy =
                AccessController.doPrivileged((PrivilegedAction<Policy>) () -> {
                    try {
                        return Policy.getInstance("JavaPolicy", new URIParameter(emptyPolicyFile.toUri()));
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                });
        IOUtils.rm(emptyPolicyFile);

        // parse the extension's policy file into a set of permissions
        final Policy policy =
                AccessController.doPrivileged((PrivilegedAction<Policy>) () -> {
                            try {
                                return Policy.getInstance("JavaPolicy", new URIParameter(file.toUri()));
                            } catch (NoSuchAlgorithmException e) {
                                throw new RuntimeException(e);
                            }
                });
        PermissionCollection permissions = policy.getPermissions(XPackExtensionSecurity.class.getProtectionDomain());
        // this method is supported with the specific implementation we use, but just check for safety.
        if (permissions == Policy.UNSUPPORTED_EMPTY_COLLECTION) {
            throw new UnsupportedOperationException("JavaPolicy implementation does not support retrieving permissions");
        }
        PermissionCollection actualPermissions = new Permissions();
        for (Permission permission : Collections.list(permissions.elements())) {
            if (!emptyPolicy.implies(XPackExtensionSecurity.class.getProtectionDomain(), permission)) {
                actualPermissions.add(permission);
            }
        }
        actualPermissions.setReadOnly();
        return actualPermissions;
    }


    /**
     * Reads extension policy, prints/confirms exceptions
     */
    static void readPolicy(Path file, Terminal terminal, Environment environment, boolean batch) throws IOException {
        PermissionCollection permissions = parsePermissions(file, environment.tmpFile());
        List<Permission> requested = Collections.list(permissions.elements());
        if (requested.isEmpty()) {
            terminal.println(Terminal.Verbosity.VERBOSE, "extension has a policy file with no additional permissions");
            return;
        }

        // sort permissions in a reasonable order
        Collections.sort(requested, new Comparator<Permission>() {
            @Override
            public int compare(Permission o1, Permission o2) {
                int cmp = o1.getClass().getName().compareTo(o2.getClass().getName());
                if (cmp == 0) {
                    String name1 = o1.getName();
                    String name2 = o2.getName();
                    if (name1 == null) {
                        name1 = "";
                    }
                    if (name2 == null) {
                        name2 = "";
                    }
                    cmp = name1.compareTo(name2);
                    if (cmp == 0) {
                        String actions1 = o1.getActions();
                        String actions2 = o2.getActions();
                        if (actions1 == null) {
                            actions1 = "";
                        }
                        if (actions2 == null) {
                            actions2 = "";
                        }
                        cmp = actions1.compareTo(actions2);
                    }
                }
                return cmp;
            }
        });

        terminal.println(Terminal.Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        terminal.println(Terminal.Verbosity.NORMAL, "@     WARNING: x-pack extension requires additional permissions     @");
        terminal.println(Terminal.Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        // print all permissions:
        for (Permission permission : requested) {
            terminal.println(Terminal.Verbosity.NORMAL, "* " + formatPermission(permission));
        }
        terminal.println(Terminal.Verbosity.NORMAL, "See http://docs.oracle.com/javase/8/docs/technotes/guides/security/permissions.html");
        terminal.println(Terminal.Verbosity.NORMAL, "for descriptions of what these permissions allow and the associated risks.");
        if (!batch) {
            terminal.println(Terminal.Verbosity.NORMAL, "");
            String text = terminal.readText("Continue with installation? [y/N]");
            if (!text.equalsIgnoreCase("y")) {
                throw new RuntimeException("installation aborted by user");
            }
        }
    }
}
