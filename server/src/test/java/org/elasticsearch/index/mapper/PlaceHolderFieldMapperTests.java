/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import static org.hamcrest.Matchers.instanceOf;

public class PlaceHolderFieldMapperTests extends MapperServiceTestCase {

    // checks that parameters of unknown field types are preserved on legacy indices
    public void testPreserveParams() throws Exception {
        XContentBuilder mapping = mapping(b -> {
            b.startObject("myfield");
            b.field("type", "unknown");
            b.field("someparam", "value");
            b.endObject();
        });
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping);
        assertThat(service.fieldType("myfield"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertEquals(Strings.toString(mapping), Strings.toString(service.documentMapper().mapping()));

        // check that field can be updated
        mapping = mapping(b -> {
            b.startObject("myfield");
            b.field("type", "unknown");
            b.field("someparam", "other");
            b.endObject();
        });
        merge(service, mapping);
        assertThat(service.fieldType("myfield"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertEquals(Strings.toString(mapping), Strings.toString(service.documentMapper().mapping()));
    }
}
