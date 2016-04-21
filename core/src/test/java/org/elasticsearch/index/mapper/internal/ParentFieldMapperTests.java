/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.index.DocValuesType;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ParentFieldMapperTests extends ESTestCase {

    public void testPost2Dot0LazyLoading() {
        ParentFieldMapper.Builder builder = new ParentFieldMapper.Builder("child");
        builder.type("parent");
        builder.eagerGlobalOrdinals(false);

        ParentFieldMapper parentFieldMapper = builder.build(new Mapper.BuilderContext(post2Dot0IndexSettings(), new ContentPath(0)));

        assertThat(parentFieldMapper.getParentJoinFieldType().name(), equalTo("_parent#child"));
        assertThat(parentFieldMapper.getParentJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getParentJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));

        assertThat(parentFieldMapper.fieldType().name(), equalTo("_parent#parent"));
        assertThat(parentFieldMapper.fieldType().eagerGlobalOrdinals(), equalTo(false));
        assertThat(parentFieldMapper.fieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.fieldType().docValuesType(), equalTo(DocValuesType.SORTED));
    }

    public void testPost2Dot0EagerLoading() {
        ParentFieldMapper.Builder builder = new ParentFieldMapper.Builder("child");
        builder.type("parent");
        builder.eagerGlobalOrdinals(true);

        ParentFieldMapper parentFieldMapper = builder.build(new Mapper.BuilderContext(post2Dot0IndexSettings(), new ContentPath(0)));

        assertThat(parentFieldMapper.getParentJoinFieldType().name(), equalTo("_parent#child"));
        assertThat(parentFieldMapper.getParentJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getParentJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));

        assertThat(parentFieldMapper.fieldType().name(), equalTo("_parent#parent"));
        assertThat(parentFieldMapper.fieldType().eagerGlobalOrdinals(), equalTo(true));
        assertThat(parentFieldMapper.fieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.fieldType().docValuesType(), equalTo(DocValuesType.SORTED));
    }

    private static Settings post2Dot0IndexSettings() {
        return Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_1_0).build();
    }

}
