/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.examples.nativescript;

import org.elasticsearch.Version;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.examples.nativescript.plugin.NativeScriptExamplesPlugin;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * Main class to easily run the the plugin from a IDE.
 */
public class ElasticsearchWithPluginLauncher {

    @SuppressForbidden(reason = "not really code or a test")
    public static void main(String[] args) throws Throwable {
        System.setProperty("es.logger.prefix", "");
        Settings settings = Settings.builder()
                .put("security.manager.enabled", "false")
                .put("plugins.load_classpath_plugins", "false")
                .put("path.home", System.getProperty("es.path.home", System.getProperty("user.dir")))
                .build();

        // Setup logging using config/logging.yml
        Environment environment = InternalSettingsPreparer.prepareEnvironment(settings, Terminal.DEFAULT);
        Class.forName("org.apache.log4j.Logger");
        LogConfigurator.configure(environment.settings(), true);

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings, Version.CURRENT, Arrays.asList(NativeScriptExamplesPlugin.class));
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                node.close();
                latch.countDown();
            }
        });
        node.start();
        latch.await();
    }
}
