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

package org.elasticsearch.analysis.common;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AnalysisPainlessExtension implements PainlessExtension {

    private static final Whitelist WHITELIST =
        WhitelistLoader.loadFromResourceFiles(AnalysisPainlessExtension.class, "painless_whitelist.txt");

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Collections.singletonMap(AnalysisPredicateScript.CONTEXT, Collections.singletonList(WHITELIST));
    }
}
