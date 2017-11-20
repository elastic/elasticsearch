/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@LuceneTestCase.SuppressFileSystems("*")
public class ListXPackExtensionCommandTests extends ESTestCase {

    private Path home;
    private Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Settings settings = Settings.builder()
                .put("path.home", home)
                .build();
        env = TestEnvironment.newEnvironment(settings);
        Files.createDirectories(extensionsFile(env));
    }

    private static class MockListXPackExtensionCommand extends ListXPackExtensionCommand {

        private final Environment env;

        private MockListXPackExtensionCommand(final Environment env) {
            this.env = env;
        }

        @Override
        protected Environment createEnv(Map<String, String> settings) throws UserException {
            return env;
        }

        @Override
        protected boolean addShutdownHook() {
            return false;
        }

    }

    static String buildMultiline(String... args){
        return Arrays.asList(args).stream().collect(Collectors.joining("\n", "", "\n"));
    }

    static void buildFakeExtension(Environment env, String description, String name, String className) throws IOException {
        XPackExtensionTestUtil.writeProperties(extensionsFile(env).resolve(name),
                "description", description,
                "name", name,
                "version", "1.0",
                "xpack.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", className);
    }

    static Path extensionsFile(final Environment env) throws IOException {
        return env.pluginsFile().resolve("x-pack").resolve("extensions");
    }

    static MockTerminal listExtensions(Path home, Environment env) throws Exception {
        MockTerminal terminal = new MockTerminal();
        int status = new MockListXPackExtensionCommand(env).main(new String[] { "-Epath.home=" + home }, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }

    static MockTerminal listExtensions(Path home, Environment env, String[] args) throws Exception {
        String[] argsAndHome = new String[args.length + 1];
        System.arraycopy(args, 0, argsAndHome, 0, args.length);
        argsAndHome[args.length] = "-Epath.home=" + home;
        MockTerminal terminal = new MockTerminal();
        int status = new MockListXPackExtensionCommand(env).main(argsAndHome, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }

    public void testExtensionsDirMissing() throws Exception {
        Files.delete(extensionsFile(env));
        IOException e = expectThrows(IOException.class, () -> listExtensions(home, env));
        assertTrue(e.getMessage(), e.getMessage().contains("Extensions directory missing"));
    }

    public void testNoExtensions() throws Exception {
        MockTerminal terminal = listExtensions(home, env);
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testNoExtensionsVerbose() throws Exception {
        String[] params = { "-v" };
        MockTerminal terminal = listExtensions(home, env, params);
        assertEquals(terminal.getOutput(), buildMultiline("XPack Extensions directory: " + extensionsFile(env)));
    }

    public void testOneExtension() throws Exception {
        buildFakeExtension(env, "", "fake", "org.fake");
        MockTerminal terminal = listExtensions(home, env);
        assertEquals(terminal.getOutput(), buildMultiline("fake"));
    }

    public void testTwoExtensions() throws Exception {
        buildFakeExtension(env, "", "fake1", "org.fake1");
        buildFakeExtension(env, "", "fake2", "org.fake2");
        MockTerminal terminal = listExtensions(home, env);
        assertEquals(terminal.getOutput(), buildMultiline("fake1", "fake2"));
    }

    public void testExtensionWithVerbose() throws Exception {
        buildFakeExtension(env, "fake desc", "fake_extension", "org.fake");
        String[] params = { "-v" };
        MockTerminal terminal = listExtensions(home, env, params);
        assertEquals(terminal.getOutput(), buildMultiline("XPack Extensions directory: " + extensionsFile(env),
                "fake_extension", "- XPack Extension information:", "Name: fake_extension",
                "Description: fake desc", "Version: 1.0", " * Classname: org.fake"));
    }

    public void testExtensionWithVerboseMultipleExtensions() throws Exception {
        buildFakeExtension(env, "fake desc 1", "fake_extension1", "org.fake");
        buildFakeExtension(env, "fake desc 2", "fake_extension2", "org.fake2");
        String[] params = { "-v" };
        MockTerminal terminal = listExtensions(home, env, params);
        assertEquals(terminal.getOutput(), buildMultiline("XPack Extensions directory: " + extensionsFile(env),
                "fake_extension1", "- XPack Extension information:", "Name: fake_extension1",
                "Description: fake desc 1", "Version: 1.0", " * Classname: org.fake",
                "fake_extension2", "- XPack Extension information:", "Name: fake_extension2",
                "Description: fake desc 2", "Version: 1.0", " * Classname: org.fake2"));
    }

    public void testExtensionWithoutVerboseMultipleExtensions() throws Exception {
        buildFakeExtension(env, "fake desc 1", "fake_extension1", "org.fake");
        buildFakeExtension(env, "fake desc 2", "fake_extension2", "org.fake2");
        MockTerminal terminal = listExtensions(home, env, new String[0]);
        String output = terminal.getOutput();
        assertEquals(output, buildMultiline("fake_extension1", "fake_extension2"));
    }

    public void testExtensionWithoutDescriptorFile() throws Exception{
        Files.createDirectories(extensionsFile(env).resolve("fake1"));
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> listExtensions(home, env));
        assertEquals(e.getFile(),
                extensionsFile(env).resolve("fake1").resolve(XPackExtensionInfo.XPACK_EXTENSION_PROPERTIES).toString());
    }

    public void testExtensionWithWrongDescriptorFile() throws Exception{
        XPackExtensionTestUtil.writeProperties(extensionsFile(env).resolve("fake1"),
                "description", "fake desc");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> listExtensions(home, env));
        assertEquals(e.getMessage(), "Property [name] is missing in [" +
                extensionsFile(env).resolve("fake1")
                        .resolve(XPackExtensionInfo.XPACK_EXTENSION_PROPERTIES).toString() + "]");
    }
}
