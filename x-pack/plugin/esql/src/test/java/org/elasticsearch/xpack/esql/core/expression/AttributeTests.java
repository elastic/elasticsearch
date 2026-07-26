/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the License.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class AttributeTests extends ESTestCase {

    public void testWithQualifierChangesQualifier() {
        ReferenceAttribute attribute = new ReferenceAttribute(Source.EMPTY, "orders", "price", DataType.INTEGER);

        Attribute qualified = attribute.withQualifier("inventory");

        assertThat(qualified, not(sameInstance(attribute)));
        assertThat(qualified.qualifier(), equalTo("inventory"));
        assertThat(qualified.name(), equalTo(attribute.name()));
        assertThat(qualified.dataType(), equalTo(attribute.dataType()));
        assertThat(qualified.nullable(), equalTo(attribute.nullable()));
    }

    public void testWithQualifierReturnsSameAttributeWhenQualifierDoesNotChange() {
        ReferenceAttribute attribute = new ReferenceAttribute(Source.EMPTY, "orders", "price", DataType.INTEGER);

        assertThat(attribute.withQualifier("orders"), sameInstance(attribute));
    }
}
