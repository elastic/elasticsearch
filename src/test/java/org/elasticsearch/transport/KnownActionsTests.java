/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.shield.action.ShieldActionModule;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class KnownActionsTests extends ShieldIntegrationTest {

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
        TransportService transportService = internalCluster().getDataNodeInstance(TransportService.class);
        for (String handler : transportService.serverHandlers.keySet()) {
            if (!knownActions.contains(handler)) {
                assertThat("elasticsearch core transport handler [" + handler + "] is unknown to shield", knownHandlers, hasItem(handler));
            }
        }
    }

    @Test
    public void testAllCodeActionsAreKnown() throws Exception {
        for (String action : codeActions) {
            assertThat("elasticsearch core action [" + action + "] is unknown to shield", knownActions, hasItem(action));
        }
    }

    @Test
    public void testAllKnownActionsAreValid() {
        for (String knownAction : knownActions) {
            assertThat("shield known action [" + knownAction + "] is unknown to core", codeActions, hasItems(knownAction));
        }
    }

    @Test
    public void testAllKnownTransportHandlersAreValid() {
        TransportService transportService = internalCluster().getDataNodeInstance(TransportService.class);
        for (String knownHandler : knownHandlers) {
            assertThat("shield known action [" + knownHandler + "] is unknown to core", transportService.serverHandlers.keySet(), hasItems(knownHandler));
        }
    }

    private static ImmutableSet<String> loadKnownActions() {
        final ImmutableSet.Builder<String> knownActionsBuilder = ImmutableSet.builder();
        try (InputStream input = KnownActionsTests.class.getResourceAsStream("actions")) {
            Streams.readAllLines(input, new Callback<String>() {
                @Override
                public void handle(String action) {
                    knownActionsBuilder.add(action);
                }
            });
        } catch (IOException ioe) {
            throw new ElasticsearchIllegalStateException("could not load known actions", ioe);
        }
        return knownActionsBuilder.build();
    }

    private static ImmutableSet<String> loadKnownHandlers() {
        final ImmutableSet.Builder<String> knownHandlersBuilder = ImmutableSet.builder();
        try (InputStream input = KnownActionsTests.class.getResourceAsStream("handlers")) {
            Streams.readAllLines(input, new Callback<String>() {
                @Override
                public void handle(String action) {
                    knownHandlersBuilder.add(action);
                }
            });
        } catch (IOException ioe) {
            throw new ElasticsearchIllegalStateException("could not load known handlers", ioe);
        }
        return knownHandlersBuilder.build();
    }

    private static ImmutableSet<String> loadCodeActions() throws IOException, IllegalAccessException {
        ImmutableSet.Builder<String> actions = ImmutableSet.builder();

        // loading es core actions
        ClassPath classPath = ClassPath.from(Action.class.getClassLoader());
        loadActions(classPath, Action.class.getPackage().getName(), actions);

        // loading shield actions
        classPath = ClassPath.from(ShieldActionModule.class.getClassLoader());
        loadActions(classPath, ShieldActionModule.class.getPackage().getName(), actions);

        // also loading all actions from the licensing plugin
        classPath = ClassPath.from(LicensePlugin.class.getClassLoader());
        loadActions(classPath, LicensePlugin.class.getPackage().getName(), actions);

        return actions.build();
    }

    private static void loadActions(ClassPath classPath, String packageName, ImmutableSet.Builder<String> actions) throws IOException, IllegalAccessException {
        ImmutableSet<ClassPath.ClassInfo> infos = classPath.getTopLevelClassesRecursive(packageName);
        for (ClassPath.ClassInfo info : infos) {
            Class clazz = info.load();
            if (Action.class.isAssignableFrom(clazz)) {
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
    }
}
