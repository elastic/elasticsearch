/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.security.authc.support.mapper.ExpressionRoleMappingTests.randomRoleMapping;
import static org.hamcrest.Matchers.equalTo;

public class RoleMappingMetadataXContentSerializationTests extends AbstractChunkedSerializingTestCase<RoleMappingMetadata> {
    @Override
    protected Writeable.Reader<RoleMappingMetadata> instanceReader() {
        return RoleMappingMetadata::new;
    }

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
    protected RoleMappingMetadata doParseInstance(XContentParser parser) throws IOException {
        return RoleMappingMetadata.fromXContent(parser);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
    }

    @Override
    protected void assertEqualInstances(RoleMappingMetadata expectedInstance, RoleMappingMetadata newInstance) {
        assertThat(expectedInstance.getRoleMappings().size(), equalTo(newInstance.getRoleMappings().size()));
        for (ExpressionRoleMapping expectedExpressionRoleMapping : expectedInstance.getRoleMappings()) {
            boolean found = false;
            for (ExpressionRoleMapping newExpressionRoleMapping : newInstance.getRoleMappings()) {
                // everything equals except name, because the name is lost during deserialization of {@code RoleMappingMetadata}
                found |= newExpressionRoleMapping.isEnabled() == expectedExpressionRoleMapping.isEnabled()
                    && Objects.equals(newExpressionRoleMapping.getExpression(), expectedExpressionRoleMapping.getExpression())
                    && Objects.equals(newExpressionRoleMapping.getRoles(), expectedExpressionRoleMapping.getRoles())
                    && Objects.equals(newExpressionRoleMapping.getRoleTemplates(), expectedExpressionRoleMapping.getRoleTemplates())
                    && Objects.equals(newExpressionRoleMapping.getMetadata(), expectedExpressionRoleMapping.getMetadata());
            }
            assertTrue(found);
        }
    }
}
