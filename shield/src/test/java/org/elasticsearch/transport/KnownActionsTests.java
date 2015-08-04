/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.shield.action.ShieldActionModule;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@ClusterScope(numClientNodes = 0, numDataNodes = 1)
public class KnownActionsTests extends ShieldIntegTestCase {

    private static ImmutableSet<String> knownActions;
    private static ImmutableSet<String> knownHandlers;
    private static ImmutableSet<String> codeActions;

    @BeforeClass
    public static void init() throws Exception {
        knownActions = loadKnownActions();
        knownHandlers = loadKnownHandlers();
        codeActions = loadCodeActions();
    }

    @Test
    public void testAllTransportHandlersAreKnown() {
        TransportService transportService = internalTestCluster().getDataNodeInstance(TransportService.class);
        for (String handler : transportService.requestHandlers.keySet()) {
            if (!knownActions.contains(handler)) {
                assertThat("elasticsearch core transport handler [" + handler + "] is unknown to shield", knownHandlers, hasItem(handler));
            }
        }
    }

    @Test
    public void testAllCodeActionsAreKnown() throws Exception {
        for (String action : codeActions) {
            assertThat("classpath action [" + action + "] is unknown to shield", knownActions, hasItem(action));
        }
    }

    @Test
    public void testAllKnownActionsAreValid() {
        for (String knownAction : knownActions) {
            assertThat("shield known action [" + knownAction + "] is not among the classpath actions", codeActions, hasItems(knownAction));
        }
    }

    @Test
    public void testAllKnownTransportHandlersAreValid() {
        TransportService transportService = internalTestCluster().getDataNodeInstance(TransportService.class);
        for (String knownHandler : knownHandlers) {
            assertThat("shield known handler [" + knownHandler + "] is unknown to core", transportService.requestHandlers.keySet(), hasItems(knownHandler));
        }
    }

    public static ImmutableSet<String> loadKnownActions() {
        final ImmutableSet.Builder<String> knownActionsBuilder = ImmutableSet.builder();
        try (InputStream input = KnownActionsTests.class.getResourceAsStream("actions")) {
            Streams.readAllLines(input, new Callback<String>() {
                @Override
                public void handle(String action) {
                    knownActionsBuilder.add(action);
                }
            });
        } catch (IOException ioe) {
            throw new IllegalStateException("could not load known actions", ioe);
        }
        return knownActionsBuilder.build();
    }

    public static ImmutableSet<String> loadKnownHandlers() {
        final ImmutableSet.Builder<String> knownHandlersBuilder = ImmutableSet.builder();
        try (InputStream input = KnownActionsTests.class.getResourceAsStream("handlers")) {
            Streams.readAllLines(input, new Callback<String>() {
                @Override
                public void handle(String action) {
                    knownHandlersBuilder.add(action);
                }
            });
        } catch (IOException ioe) {
            throw new IllegalStateException("could not load known handlers", ioe);
        }
        return knownHandlersBuilder.build();
    }

    private static ImmutableSet<String> loadCodeActions() throws IOException, ReflectiveOperationException, URISyntaxException {
        ImmutableSet.Builder<String> actions = ImmutableSet.builder();

        // loading es core actions
        loadActions(collectSubClasses(Action.class, Action.class), actions);

        // loading shield actions
        loadActions(collectSubClasses(Action.class, ShieldActionModule.class), actions);

        // also loading all actions from the licensing plugin
        loadActions(collectSubClasses(Action.class, LicensePlugin.class), actions);

        return actions.build();
    }

    private static void loadActions(Collection<Class<?>> clazzes, ImmutableSet.Builder<String> actions) throws ReflectiveOperationException {
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
    @AwaitsFix(bugUrl = "fails due to security manager consraints on file write")
    private static Collection<Class<?>> collectSubClasses(Class<?> subClass, Class<?> prototype) throws IOException, ReflectiveOperationException, URISyntaxException {
        URL codeLocation = prototype.getProtectionDomain().getCodeSource().getLocation();
        final FileSystem fileSystem;
        final Path root;
        if (codeLocation.toURI().toString().endsWith(".jar")) {
            try {
                // hack around a bug in the zipfilesystem implementation before java 9,
                // its checkWritable was incorrect and it won't work without write permissions. 
                // if we add the permission, it will open jars r/w, which is too scary! so copy to a safe r-w location.
                Path tmp = createTempFile(null, ".jar");
                try (InputStream in = codeLocation.openStream()) {
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

    private static void collectClassesForPackage(Class<?> subclass, Path root, ClassLoader cld, String pckgname, List<Class<?>> classes) throws IOException, ReflectiveOperationException {
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
