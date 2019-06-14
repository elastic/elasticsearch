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

import java.util.Arrays;

/**
 * A sub-command for the keystore cli which changes the password.
 */
class ChangeKeyStorePasswordCommand extends EnvironmentAwareCommand {

    ChangeKeyStorePasswordCommand() {
        super("Changes the password of a keystore");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        char[] newPassword = null;
        try (KeystoreAndPassword keyAndPass = KeyStoreWrapper.readOrCreate(terminal, env.configFile(), false)) {
            if (null == keyAndPass) {
                return;
            }
            KeyStoreWrapper keystore = keyAndPass.getKeystore();
            newPassword = KeyStoreWrapper.readPassword(terminal, true);
            keystore.save(env.configFile(), newPassword);
            terminal.println("Elasticsearch keystore password changed successfully." + env.configFile());
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, "Failed to access the keystore. Please make sure the password was correct.", e);
        } finally {
            if (null != newPassword) {
                Arrays.fill(newPassword, '\u0000');
            }
        }
    }
}
