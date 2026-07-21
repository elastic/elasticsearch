/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import org.elasticsearch.datatree.DataArray;
import org.elasticsearch.datatree.DataBoolean;
import org.elasticsearch.datatree.DataDouble;
import org.elasticsearch.datatree.DataInteger;
import org.elasticsearch.datatree.DataLong;
import org.elasticsearch.datatree.DataObject;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class XContentDataTests extends ESTestCase {

    public void testConvertsRoleMapperExpression() throws Exception {
        FieldExpression expression = new FieldExpression("username", List.of(new FieldValue("john")));

        DataObject object = XContentData.fromXContent(expression, ToXContent.EMPTY_PARAMS);

        // { "field" : { "username" : "john" } }
        DataObject field = object.require("field").requireObject();
        assertThat(field.require("username").requireString(), equalTo("john"));
    }

    public void testConvertsMultiValuedExpressionToArray() throws Exception {
        FieldExpression expression = new FieldExpression("groups", List.of(new FieldValue("admins"), new FieldValue("operators")));

        DataObject object = XContentData.fromXContent(expression, ToXContent.EMPTY_PARAMS);

        DataArray groups = object.require("field").requireObject().require("groups").requireArray();
        assertThat(groups.size(), equalTo(2));
        assertThat(groups.get(0).requireString(), equalTo("admins"));
        assertThat(groups.get(1).requireString(), equalTo("operators"));
    }

    public void testRoleDescriptorMetadataNumbers() throws Exception {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("count", 3);
        metadata.put("ratio", 1.5d);
        metadata.put("huge", new BigInteger("12345678901234567890"));
        RoleDescriptor role = new RoleDescriptor("r", null, null, null, metadata);

        DataObject object = XContentData.fromXContent(role, ToXContent.EMPTY_PARAMS);

        DataObject parsed = object.require("metadata").requireObject();
        assertThat(parsed.require("count"), equalTo(new DataLong(3L)));
        assertThat(parsed.require("ratio"), equalTo(new DataDouble(1.5d)));
        assertThat(parsed.require("huge"), equalTo(new DataInteger(new BigInteger("12345678901234567890"))));
    }

    public void testFromBuilderPreservesFieldOrder() throws Exception {
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject().field("name", "role").field("enabled", true).array("indices", "a", "b").endObject();

        DataObject object = XContentData.fromBuilder(builder);

        assertThat(object.view().keySet(), contains("name", "enabled", "indices"));
        assertThat(object.require("enabled"), equalTo(new DataBoolean(true)));
        assertThat(object.require("indices").requireArray().size(), equalTo(2));
    }
}
