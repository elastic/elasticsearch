/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.registry;

import org.elasticsearch.inference.services.elser.ElserService;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;

public class ServiceRegistryTests extends ESTestCase {

    public void testGetService() {
        ServiceRegistry registry = new ServiceRegistry(mock(ElserService.class));
        var service = registry.getService(ElserService.NAME);
        assertTrue(service.isPresent());
    }

    public void testGetUnknownService() {
        ServiceRegistry registry = new ServiceRegistry(mock(ElserService.class));
        var service = registry.getService("foo");
        assertFalse(service.isPresent());
    }
}
