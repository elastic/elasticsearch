/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Mappings. Mappings define the way that documents should be translated to
 * Lucene indices, for instance whether a string field should be indexed as a
 * {@link org.elasticsearch.index.mapper.TextFieldMapper text} or
 * {@link org.elasticsearch.index.mapper.KeywordFieldMapper keyword} field,
 * etc. This parsing is done by the
 * {@link org.elasticsearch.index.mapper.DocumentParser} class which delegates
 * to various {@link org.elasticsearch.index.mapper.Mapper} implementations for
 * per-field handling.
 * <p>Mappings support the addition of new fields, so that fields can be added
 * to indices even though users had not thought about them at index creation
 * time. However, the removal of fields is not supported, as it would allow to
 * re-add a field with a different configuration under the same name, which
 * Lucene cannot handle. Introduction of new fields into the mappings is handled
 * by the {@link org.elasticsearch.index.mapper.MapperService} class.
 */
package org.elasticsearch.index.mapper;

