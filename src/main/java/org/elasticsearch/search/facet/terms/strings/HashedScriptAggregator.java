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
package org.elasticsearch.search.facet.terms.strings;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.script.SearchScript;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HashedScriptAggregator extends HashedAggregator {

    private final ImmutableSet<BytesRef> excluded;
    private final Matcher matcher;
    private final SearchScript script;
    private final CharsRef spare = new CharsRef();
    private final BytesRef scriptSpare = new BytesRef();
    private final boolean convert;
    
    public HashedScriptAggregator(ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
        this.excluded = excluded;
        this.matcher = pattern != null ? pattern.matcher("") : null;
        this.script = script;
        this.convert = script != null || matcher != null;
    }

    @Override
    public void addValue(BytesRef value, int hashCode) {
        if (accept(value)) {
            super.addValue(value, hashCode);
        }
    }
    
    private boolean accept(BytesRef value) {
        if (excluded != null && excluded.contains(value)) {
            return false;
        }
        if(convert) {
            // only convert if we need to and only once per doc...
            UnicodeUtil.UTF8toUTF16(value, spare);
            if (matcher != null) {
                assert convert : "regexp: [convert == false] but should be true";
                assert value.utf8ToString().equals(spare.toString()) : "not converted";
                return matcher.reset(spare).matches();
            }
        }
        return true;
    }
    
    @Override
    protected void onValue(int docId, BytesRef value, int hashCode) {
        if (accept(value)) {
            if (script != null) {
                assert convert : "script: [convert == false] but should be true";
                assert value.utf8ToString().equals(spare.toString()) : "not converted";
                script.setNextDocId(docId);
                // LUCENE 4 UPGRADE: needs optimization -- maybe a CharSequence
                // does the job here?
                // we only create that string if we really need
                script.setNextVar("term", spare.toString());
                Object scriptValue = script.run();
                if (scriptValue == null) {
                    return;
                }
                if (scriptValue instanceof Boolean) {
                    if (!((Boolean) scriptValue)) {
                        return;
                    }
                } else {
                    scriptSpare.copyChars(scriptValue.toString());
                    hashCode = scriptSpare.hashCode();
                    super.onValue(docId, scriptSpare, hashCode);
                    return;
                }
            }
            assert convert || (matcher == null && script == null);
            super.onValue(docId, value, hashCode);
        }
    }
}