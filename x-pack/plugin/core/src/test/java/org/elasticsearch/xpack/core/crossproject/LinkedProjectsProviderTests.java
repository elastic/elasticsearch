/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.crossproject;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.LinkedProjectConfig.ProxyLinkedProjectConfigBuilder;
import org.elasticsearch.transport.LinkedProjectConfigService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class LinkedProjectsProviderTests extends ESTestCase {

    private static class TestLinkedProjectConfigService implements LinkedProjectConfigService {
        private final List<LinkedProjectConfig> initialConfigs;
        private final List<LinkedProjectConfigListener> listeners = new ArrayList<>();

        TestLinkedProjectConfigService(List<LinkedProjectConfig> initialConfigs) {
            this.initialConfigs = initialConfigs;
        }

        @Override
        public void register(LinkedProjectConfigListener listener) {
            listeners.add(listener);
        }

        @Override
        public Collection<LinkedProjectConfig> getInitialLinkedProjectConfigs() {
            return initialConfigs;
        }

        void fireUpdate(LinkedProjectConfig config) {
            listeners.forEach(l -> l.updateLinkedProject(config));
        }

        void fireRemove(ProjectId originProjectId, ProjectId linkedProjectId, String alias) {
            listeners.forEach(l -> l.remove(originProjectId, linkedProjectId, alias));
        }
    }

    private static LinkedProjectConfig config(ProjectId origin, ProjectId linked, String alias) {
        return new ProxyLinkedProjectConfigBuilder(origin, linked, alias).proxyAddress("localhost:9300").build();
    }

    public void testSingleProject_emptyInitial() {
        var service = new TestLinkedProjectConfigService(List.of());
        var provider = new LinkedProjectsProvider.SingleProjectLinkedProjectsProvider(service);
        assertTrue(provider.getLinkedProjects(ProjectId.DEFAULT).isEmpty());
    }

    public void testSingleProject_withInitialConfig() {
        var linked = ProjectId.fromId("linked-1");
        var service = new TestLinkedProjectConfigService(List.of(config(ProjectId.DEFAULT, linked, "alias-1")));
        var provider = new LinkedProjectsProvider.SingleProjectLinkedProjectsProvider(service);
        assertEquals(Set.of(linked), provider.getLinkedProjects(ProjectId.DEFAULT));
    }

    public void testSingleProject_update() {
        var service = new TestLinkedProjectConfigService(List.of());
        var provider = new LinkedProjectsProvider.SingleProjectLinkedProjectsProvider(service);
        assertTrue(provider.getLinkedProjects(ProjectId.DEFAULT).isEmpty());

        var linked = ProjectId.fromId("linked-1");
        service.fireUpdate(config(ProjectId.DEFAULT, linked, "alias-1"));
        assertEquals(Set.of(linked), provider.getLinkedProjects(ProjectId.DEFAULT));
    }

    public void testSingleProject_remove() {
        var linked = ProjectId.fromId("linked-1");
        var service = new TestLinkedProjectConfigService(List.of(config(ProjectId.DEFAULT, linked, "alias-1")));
        var provider = new LinkedProjectsProvider.SingleProjectLinkedProjectsProvider(service);
        assertFalse(provider.getLinkedProjects(ProjectId.DEFAULT).isEmpty());

        service.fireRemove(ProjectId.DEFAULT, linked, "alias-1");
        assertTrue(provider.getLinkedProjects(ProjectId.DEFAULT).isEmpty());
    }

    public void testMultiProject_emptyInitial() {
        var service = new TestLinkedProjectConfigService(List.of());
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);
        assertTrue(provider.getLinkedProjects(ProjectId.DEFAULT).isEmpty());
    }

    public void testMultiProject_withInitialConfig() {
        var origin = ProjectId.fromId("origin-1");
        var linked = ProjectId.fromId("linked-1");
        var service = new TestLinkedProjectConfigService(List.of(config(origin, linked, "alias-1")));
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);
        assertEquals(Set.of(linked), provider.getLinkedProjects(origin));
    }

    public void testMultiProject_unknownOriginReturnsEmpty() {
        var origin = ProjectId.fromId("origin-1");
        var linked = ProjectId.fromId("linked-1");
        var service = new TestLinkedProjectConfigService(List.of(config(origin, linked, "alias-1")));
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);
        assertTrue(provider.getLinkedProjects(ProjectId.fromId("unknown-origin")).isEmpty());
    }

    public void testMultiProject_multipleOriginsAreIsolated() {
        var origin1 = ProjectId.fromId("origin-1");
        var linked1 = ProjectId.fromId("linked-1");
        var origin2 = ProjectId.fromId("origin-2");
        var linked2 = ProjectId.fromId("linked-2");
        var service = new TestLinkedProjectConfigService(List.of(config(origin1, linked1, "alias-1"), config(origin2, linked2, "alias-2")));
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);
        assertEquals(Set.of(linked1), provider.getLinkedProjects(origin1));
        assertEquals(Set.of(linked2), provider.getLinkedProjects(origin2));
    }

    public void testMultiProject_update() {
        var origin = ProjectId.fromId("origin-1");
        var service = new TestLinkedProjectConfigService(List.of());
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);
        assertTrue(provider.getLinkedProjects(origin).isEmpty());

        var linked = ProjectId.fromId("linked-1");
        service.fireUpdate(config(origin, linked, "alias-1"));
        assertEquals(Set.of(linked), provider.getLinkedProjects(origin));
    }

    public void testMultiProject_remove() {
        var origin = ProjectId.fromId("origin-1");
        var linked = ProjectId.fromId("linked-1");
        var service = new TestLinkedProjectConfigService(List.of(config(origin, linked, "alias-1")));
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);
        assertFalse(provider.getLinkedProjects(origin).isEmpty());

        service.fireRemove(origin, linked, "alias-1");
        assertTrue(provider.getLinkedProjects(origin).isEmpty());
    }

    public void testMultiProject_removeDoesNotAffectOtherOrigins() {
        var origin1 = ProjectId.fromId("origin-1");
        var linked1 = ProjectId.fromId("linked-1");
        var origin2 = ProjectId.fromId("origin-2");
        var linked2 = ProjectId.fromId("linked-2");
        var service = new TestLinkedProjectConfigService(List.of(config(origin1, linked1, "alias-1"), config(origin2, linked2, "alias-2")));
        var provider = new LinkedProjectsProvider.MultiProjectLinkedProjectsProvider(service);

        service.fireRemove(origin1, linked1, "alias-1");
        assertTrue(provider.getLinkedProjects(origin1).isEmpty());
        assertEquals(Set.of(linked2), provider.getLinkedProjects(origin2));
    }

}
