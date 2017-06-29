/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.license.Licensing;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.deprecation.Deprecation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false, numDataNodes = 1)
public class KnownActionsTests extends SecurityIntegTestCase {
    private static Set<String> knownActions;
    private static Set<String> knownHandlers;
    private static Set<String> codeActions;

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = super.getMockPlugins();
        // no handler wrapping here we check the requestHandlers below and this plugin wraps it
        return mockPlugins.stream().filter(p -> p != AssertingTransportInterceptor.TestPlugin.class).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false).build();
    }

    @BeforeClass
    public static void init() throws Exception {
        knownActions = loadKnownActions();
        knownHandlers = loadKnownHandlers();
        codeActions = loadCodeActions();
    }

    public void testAllTransportHandlersAreKnown() {
        TransportService transportService = internalCluster().getDataNodeInstance(TransportService.class);
        for (String handler : transportService.requestHandlers.keySet()) {
            if (!knownActions.contains(handler)) {
                assertThat("elasticsearch core transport handler [" + handler + "] is unknown to security", knownHandlers,
                        hasItem(handler));
            }
        }
    }

    public void testAllCodeActionsAreKnown() throws Exception {
        for (String action : codeActions) {
            assertThat("classpath action [" + action + "] is unknown to security", knownActions, hasItem(action));
        }
    }

    public void testAllKnownActionsAreValid() {
        for (String knownAction : knownActions) {
            assertThat("security known action [" + knownAction + "] is not among the classpath actions", codeActions,
                    hasItems(knownAction));
        }
    }

    public void testAllKnownTransportHandlersAreValid() {
        TransportService transportService = internalCluster().getDataNodeInstance(TransportService.class);
        for (String knownHandler : knownHandlers) {
            assertThat("security known handler [" + knownHandler + "] is unknown to core", transportService.requestHandlers.keySet(),
                    hasItems(knownHandler));
        }
    }

    public static Set<String> loadKnownActions() {
        return readSetFromResource("actions");
    }

    public static Set<String> loadKnownHandlers() {
        return readSetFromResource("handlers");
    }

    private static Set<String> readSetFromResource(String resource) {
        Set<String> knownActions = new HashSet<>();
        try (InputStream input = KnownActionsTests.class.getResourceAsStream(resource)) {
            Streams.readAllLines(input, action -> knownActions.add(action));
        } catch (IOException ioe) {
            throw new IllegalStateException("could not load known " + resource, ioe);
        }
        return unmodifiableSet(knownActions);
    }

    private static Set<String> loadCodeActions() throws IOException, ReflectiveOperationException, URISyntaxException {
        Set<String> actions = new HashSet<>();

        // loading es core actions in org.elasticsearch package
        loadActions(collectSubClasses(Action.class, Version.class), actions);

        // loading all xpack top level actions in org.elasticsearch.xpack package
        loadActions(collectSubClasses(Action.class, XPackPlugin.class), actions);

        // also loading all actions from the licensing plugin in org.elasticsearch.license package
        loadActions(collectSubClasses(Action.class, Licensing.class), actions);

        // also load stuff from Reindex in org.elasticsearch.index.reindex package
        loadActions(collectSubClasses(Action.class, ReindexPlugin.class), actions);

        // also load stuff from Deprecation in org.elasticsearch.deprecation
        loadActions(collectSubClasses(Action.class, Deprecation.class), actions);

        return unmodifiableSet(actions);
    }

    private static void loadActions(Collection<Class<?>> clazzes, Set<String> actions) throws ReflectiveOperationException {
        for (Class<?> clazz : clazzes) {
            if (!Modifier.isAbstract(clazz.getModifiers())) {
                Field field = null;
                try {
                    field = clazz.getField("INSTANCE");
                } catch (NoSuchFieldException nsfe) {
                    fail("every action should have a static field called INSTANCE, missing in " + clazz.getName());
                }
                assertThat("every action should have a static field called INSTANCE, present but not static in " + clazz.getName(),
                        Modifier.isStatic(field.getModifiers()), is(true));
                actions.add(((Action) field.get(null)).name());
            }
        }
    }

    /**
     * finds all subclasses extending {@code subClass}, recursively from the package and codesource of {@code prototype}
     */
    private static Collection<Class<?>> collectSubClasses(Class<?> subClass, Class<?> prototype) throws IOException,
            ReflectiveOperationException, URISyntaxException {
        URL codeLocation = prototype.getProtectionDomain().getCodeSource().getLocation();
        final FileSystem fileSystem;
        final Path root;
        if (codeLocation.toURI().toString().endsWith(".jar")) {
            try {
                // hack around a bug in the zipfilesystem implementation before java 9,
                // its checkWritable was incorrect and it won't work without write permissions.
                // if we add the permission, it will open jars r/w, which is too scary! so copy to a safe r-w location.
                Path tmp = createTempFile(null, ".jar");
                try (InputStream in = FileSystemUtils.openFileURLStream(codeLocation)) {
                    Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                }
                fileSystem = FileSystems.newFileSystem(new URI("jar:" + tmp.toUri()), Collections.<String,Object>emptyMap());
                root = fileSystem.getPath("/");
            } catch (URISyntaxException e) {
                throw new IOException("couldn't open zipfilesystem: ", e);
            }
        } else {
            fileSystem = null;
            root = PathUtils.get(codeLocation.toURI());
        }
        ClassLoader loader = prototype.getClassLoader();
        List<Class<?>> clazzes = new ArrayList<>();
        try {
            collectClassesForPackage(subClass, root, loader, prototype.getPackage().getName(), clazzes);
        } finally {
            IOUtils.close(fileSystem);
        }
        return clazzes;
    }

    private static void collectClassesForPackage(Class<?> subclass, Path root, ClassLoader cld, String pckgname, List<Class<?>> classes)
            throws IOException, ReflectiveOperationException {
        String pathName = pckgname.replace('.', '/');
        Path directory = root.resolve(pathName);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                if (Files.isDirectory(file)) {
                    // recurse
                    String subPackage = pckgname + "." + file.getFileName().toString();
                    // remove trailing / or whatever
                    if (subPackage.endsWith(root.getFileSystem().getSeparator())) {
                        subPackage = subPackage.substring(0, subPackage.length() - root.getFileSystem().getSeparator().length());
                    }
                    collectClassesForPackage(subclass, root, cld, subPackage, classes);
                }
                String fname = file.getFileName().toString();
                if (fname.endsWith(".class")) {
                    String clazzName = fname.substring(0, fname.length() - 6);
                    Class<?> clazz = Class.forName(pckgname + '.' + clazzName, false, cld);
                    // Don't run static initializers, as we won't use most of them.
                    // Java will do that automatically once accessed/instantiated.
                    if (subclass.isAssignableFrom(clazz)) {
                        classes.add(clazz);
                    }
                }
            }
        }
    }
}
