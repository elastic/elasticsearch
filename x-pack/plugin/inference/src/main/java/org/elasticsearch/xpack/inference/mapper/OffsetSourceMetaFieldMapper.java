/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

/**
 * This meta field only exists because offset source fields index everything into a
 * common _offset_source field and Elasticsearch has a custom codec that complains
 * when fields exist in the index and not in mappings.
 */
public class OffsetSourceMetaFieldMapper extends MetadataFieldMapper {

  public static final String NAME = "_offset_source";

  public static final String CONTENT_TYPE = "_offset_source";

  public static final TypeParser PARSER = new FixedTypeParser(c -> new OffsetSourceMetaFieldMapper());

  public static final class OffsetSourceMetaFieldType extends MappedFieldType {

    public static final OffsetSourceMetaFieldType INSTANCE = new OffsetSourceMetaFieldType();

    // made visible for tests
    OffsetSourceMetaFieldType() {
      super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
    }

    @Override
    public String typeName() {
      return CONTENT_TYPE;
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
      throw new UnsupportedOperationException("Cannot fetch values for internal field [" + typeName() + "].");
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
      throw new UnsupportedOperationException("Cannot run exists query on [_offset_source]");
    }

    @Override
    public boolean fieldHasValue(FieldInfos fieldInfos) {
      return fieldInfos.fieldInfo(NAME) != null;
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
      throw new UnsupportedOperationException("The [_offset_source] field may not be queried directly");
    }
  }

  private OffsetSourceMetaFieldMapper() {
    super(OffsetSourceMetaFieldType.INSTANCE);
  }

  @Override
  protected String contentType() {
    return CONTENT_TYPE;
  }
}