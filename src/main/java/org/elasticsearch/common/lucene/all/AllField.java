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

package org.elasticsearch.common.lucene.all;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.ElasticsearchException;

import java.io.IOException;
import java.io.Reader;

/**
 *
 */
public class AllField extends Field {

    private final AllEntries allEntries;

    private final Analyzer analyzer;

    public AllField(String name, AllEntries allEntries, Analyzer analyzer, FieldType fieldType) {
        super(name, fieldType);
        this.allEntries = allEntries;
        this.analyzer = analyzer;
    }

    @Override
    public String stringValue() {
        if (fieldType().stored()) {
            return allEntries.buildText();
        }
        return null;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer) throws IOException {
        try {
            allEntries.reset(); // reset the all entries, just in case it was read already
            return AllTokenStream.allTokenStream(name, allEntries, analyzer);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create token stream");
        }
    }
}
