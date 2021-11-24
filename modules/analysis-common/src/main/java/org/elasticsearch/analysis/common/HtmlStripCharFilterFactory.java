/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractCharFilterFactory;

import java.io.Reader;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

public class HtmlStripCharFilterFactory extends AbstractCharFilterFactory {
    private final Set<String> escapedTags;

    HtmlStripCharFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name);
        List<String> escapedTagsList = settings.getAsList("escaped_tags");
        if (escapedTagsList.size() > 0) {
            this.escapedTags = unmodifiableSet(newHashSet(escapedTagsList));
        } else {
            this.escapedTags = null;
        }
    }

    @Override
    public Reader create(Reader tokenStream) {
        return new HTMLStripCharFilter(tokenStream, escapedTags);
    }
}
