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
package org.elasticsearch.benchmark.scripts.score.plugin;

import org.elasticsearch.benchmark.scripts.score.script.*;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;

public class NativeScriptExamplesPlugin extends AbstractPlugin {


    @Override
    public String name() {
        return "native-script-example";
    }

    @Override
    public String description() {
        return "Native script examples";
    }

    public void onModule(ScriptModule module) {
        module.registerScript(NativeNaiveTFIDFScoreScript.NATIVE_NAIVE_TFIDF_SCRIPT_SCORE, NativeNaiveTFIDFScoreScript.Factory.class);
        module.registerScript(NativeConstantForLoopScoreScript.NATIVE_CONSTANT_FOR_LOOP_SCRIPT_SCORE, NativeConstantForLoopScoreScript.Factory.class);
        module.registerScript(NativeConstantScoreScript.NATIVE_CONSTANT_SCRIPT_SCORE, NativeConstantScoreScript.Factory.class);
        module.registerScript(NativePayloadSumScoreScript.NATIVE_PAYLOAD_SUM_SCRIPT_SCORE, NativePayloadSumScoreScript.Factory.class);
        module.registerScript(NativePayloadSumNoRecordScoreScript.NATIVE_PAYLOAD_SUM_NO_RECORD_SCRIPT_SCORE, NativePayloadSumNoRecordScoreScript.Factory.class);
    }
}
