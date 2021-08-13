/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.mapper;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.fetch.subphase.highlight.AnnotatedTextHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;

import java.util.Collections;
import java.util.Map;

public class AnnotatedTextPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(AnnotatedTextFieldMapper.CONTENT_TYPE, AnnotatedTextFieldMapper.PARSER);
    }

    @Override
    public Map<String, Highlighter> getHighlighters() {
        return Collections.singletonMap(AnnotatedTextHighlighter.NAME, new AnnotatedTextHighlighter());
    }
}
