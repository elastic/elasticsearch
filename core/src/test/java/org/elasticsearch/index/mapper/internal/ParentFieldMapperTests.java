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
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType.Loading;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ParentFieldMapperTests extends ESTestCase {

    public void testPost2Dot0LazyLoading() {
        ParentFieldMapper.Builder builder = new ParentFieldMapper.Builder("child");
        builder.type("parent");
        builder.fieldDataSettings(createFDSettings(Loading.LAZY));

        ParentFieldMapper parentFieldMapper = builder.build(new Mapper.BuilderContext(post2Dot0IndexSettings(), new ContentPath(0)));

        assertThat(parentFieldMapper.getParentJoinFieldType().names().indexName(), equalTo("_parent#child"));
        assertThat(parentFieldMapper.getParentJoinFieldType().fieldDataType(), nullValue());
        assertThat(parentFieldMapper.getParentJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getParentJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));

        assertThat(parentFieldMapper.getChildJoinFieldType().names().indexName(), equalTo("_parent#parent"));
        assertThat(parentFieldMapper.getChildJoinFieldType().fieldDataType().getLoading(), equalTo(Loading.LAZY));
        assertThat(parentFieldMapper.getChildJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getChildJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));
    }

    public void testPost2Dot0EagerLoading() {
        ParentFieldMapper.Builder builder = new ParentFieldMapper.Builder("child");
        builder.type("parent");
        builder.fieldDataSettings(createFDSettings(Loading.EAGER));

        ParentFieldMapper parentFieldMapper = builder.build(new Mapper.BuilderContext(post2Dot0IndexSettings(), new ContentPath(0)));

        assertThat(parentFieldMapper.getParentJoinFieldType().names().indexName(), equalTo("_parent#child"));
        assertThat(parentFieldMapper.getParentJoinFieldType().fieldDataType(), nullValue());
        assertThat(parentFieldMapper.getParentJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getParentJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));

        assertThat(parentFieldMapper.getChildJoinFieldType().names().indexName(), equalTo("_parent#parent"));
        assertThat(parentFieldMapper.getChildJoinFieldType().fieldDataType().getLoading(), equalTo(Loading.EAGER));
        assertThat(parentFieldMapper.getChildJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getChildJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));
    }

    public void testPost2Dot0EagerGlobalOrdinalsLoading() {
        ParentFieldMapper.Builder builder = new ParentFieldMapper.Builder("child");
        builder.type("parent");
        builder.fieldDataSettings(createFDSettings(Loading.EAGER_GLOBAL_ORDINALS));

        ParentFieldMapper parentFieldMapper = builder.build(new Mapper.BuilderContext(post2Dot0IndexSettings(), new ContentPath(0)));

        assertThat(parentFieldMapper.getParentJoinFieldType().names().indexName(), equalTo("_parent#child"));
        assertThat(parentFieldMapper.getParentJoinFieldType().fieldDataType(), nullValue());
        assertThat(parentFieldMapper.getParentJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getParentJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));

        assertThat(parentFieldMapper.getChildJoinFieldType().names().indexName(), equalTo("_parent#parent"));
        assertThat(parentFieldMapper.getChildJoinFieldType().fieldDataType().getLoading(), equalTo(Loading.EAGER_GLOBAL_ORDINALS));
        assertThat(parentFieldMapper.getChildJoinFieldType().hasDocValues(), is(true));
        assertThat(parentFieldMapper.getChildJoinFieldType().docValuesType(), equalTo(DocValuesType.SORTED));
    }

    private static Settings post2Dot0IndexSettings() {
        return Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_1_0).build();
    }

    private static Settings createFDSettings(Loading loading) {
        return new FieldDataType("child", settingsBuilder().put(Loading.KEY, loading)).getSettings();
    }

}
