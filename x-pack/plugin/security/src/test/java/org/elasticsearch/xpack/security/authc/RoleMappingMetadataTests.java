/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.security.authc.support.mapper.ExpressionRoleMappingTests.randomRoleMapping;
import static org.hamcrest.Matchers.is;

public class RoleMappingMetadataTests extends AbstractWireSerializingTestCase<RoleMappingMetadata> {

    @Override
    protected RoleMappingMetadata createTestInstance() {
        return new RoleMappingMetadata(randomSet(0, 3, () -> randomRoleMapping(true)));
    }

    @Override
    protected RoleMappingMetadata mutateInstance(RoleMappingMetadata instance) throws IOException {
        Set<ExpressionRoleMapping> mutatedRoleMappings = new HashSet<>(instance.getRoleMappings());
        boolean mutated = false;
        if (mutatedRoleMappings.isEmpty() == false && randomBoolean()) {
            mutated = true;
            mutatedRoleMappings.remove(randomFrom(mutatedRoleMappings));
        }
        if (randomBoolean() || mutated == false) {
            mutatedRoleMappings.add(randomRoleMapping(true));
        }
        return new RoleMappingMetadata(mutatedRoleMappings);
    }

    @Override
    protected Writeable.Reader<RoleMappingMetadata> instanceReader() {
        return RoleMappingMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
    }

    public void testEquals() {
        Set<ExpressionRoleMapping> roleMappings1 = randomSet(0, 3, () -> randomRoleMapping(true));
        Set<ExpressionRoleMapping> roleMappings2 = randomSet(0, 3, () -> randomRoleMapping(true));
        assumeFalse("take 2 different role mappings", roleMappings1.equals(roleMappings2));
        assertThat(new RoleMappingMetadata(roleMappings1).equals(new RoleMappingMetadata(roleMappings2)), is(false));
        assertThat(new RoleMappingMetadata(roleMappings1).equals(new RoleMappingMetadata(roleMappings1)), is(true));
        assertThat(new RoleMappingMetadata(roleMappings2).equals(new RoleMappingMetadata(roleMappings2)), is(true));
    }
}
