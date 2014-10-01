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
package org.elasticsearch.script;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ScriptServiceTests extends ElasticsearchTestCase {

    @Test
    public void testScriptsWithoutExtensions() throws IOException {
        File homeFolder = newTempDir();
        File genericConfigFolder = newTempDir();

        Settings settings = settingsBuilder()
                .put("path.conf", genericConfigFolder)
                .put("path.home", homeFolder)
                .build();
        Environment environment = new Environment(settings);

        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, null);

        logger.info("--> setup script service");
        ScriptService scriptService = new ScriptService(settings, environment, ImmutableSet.of(new TestEngineService()), resourceWatcherService);
        File scriptsFile = new File(genericConfigFolder, "scripts");
        assertThat(scriptsFile.mkdir(), equalTo(true));
        resourceWatcherService.notifyNow();

        logger.info("--> setup two test files one with extension and another without");
        File testFileNoExt = new File(scriptsFile, "test_no_ext");
        File testFileWithExt = new File(scriptsFile, "test_script.tst");
        Streams.copy("test_file_no_ext".getBytes("UTF-8"), testFileNoExt);
        Streams.copy("test_file".getBytes("UTF-8"), testFileWithExt);
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly processed");
        CompiledScript compiledScript = scriptService.compile("test", "test_script", ScriptService.ScriptType.FILE);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        logger.info("--> delete both files");
        assertThat(testFileNoExt.delete(), equalTo(true));
        assertThat(testFileWithExt.delete(), equalTo(true));
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly removed");
        try {
            scriptService.compile("test", "test_script", ScriptService.ScriptType.FILE);
            fail("the script test_script should no longe exist");
        } catch (ElasticsearchIllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("Unable to find on disk script test_script"));
        }
    }

    public static class TestEngineService implements ScriptEngineService {

        @Override
        public String[] types() {
            return new String[] {"test"};
        }

        @Override
        public String[] extensions() {
            return new String[] {"test", "tst"};
        }

        @Override
        public boolean sandboxed() {
            return false;
        }

        @Override
        public Object compile(String script) {
            return "compiled_" + script;
        }

        @Override
        public ExecutableScript executable(final Object compiledScript, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public Object execute(Object compiledScript, Map<String, Object> vars) {
            return null;
        }

        @Override
        public Object unwrap(Object value) {
            return null;
        }

        @Override
        public void close() {

        }
    }

}
