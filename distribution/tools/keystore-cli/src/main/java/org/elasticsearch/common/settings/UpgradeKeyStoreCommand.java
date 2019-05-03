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

package org.elasticsearch.common.settings;

import joptsimple.OptionSet;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

/**
 * A sub-command for the keystore CLI that enables upgrading the keystore format.
 */
public class UpgradeKeyStoreCommand extends EnvironmentAwareCommand {

    UpgradeKeyStoreCommand() {
        super("Upgrade the keystore format");
    }

    @Override
    protected void execute(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        final KeyStoreWrapper wrapper = KeyStoreWrapper.load(env.configFile());
        if (wrapper == null) {
            throw new UserException(
                    ExitCodes.CONFIG,
                    "keystore does not exist at [" + KeyStoreWrapper.keystorePath(env.configFile()) + "]");
        }
        wrapper.decrypt(new char[0]);
        KeyStoreWrapper.upgrade(wrapper, env.configFile(), new char[0]);
    }

}
