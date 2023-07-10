/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.test.ESTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractSystemIndexFormatVersionTests extends ESTestCase {
    public abstract Collection<SystemIndexDescriptor> getSystemIndexDescriptors();

    public void testSystemIndexDescriptorFormats() {
        for (SystemIndexDescriptor descriptor : getSystemIndexDescriptors()) {
            assertTrue(descriptor.isAutomaticallyManaged());
            assertThat(descriptor.getIndexFormat().hash(), is(notNullValue()));
        }
    }
}
