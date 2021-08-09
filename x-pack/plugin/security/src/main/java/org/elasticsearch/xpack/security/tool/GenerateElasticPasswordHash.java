/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.tool;

import joptsimple.OptionSet;

import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class GenerateElasticPasswordHash extends EnvironmentAwareCommand {

    public GenerateElasticPasswordHash() {
        super("Generates a password hash for for the elastic user and stores it in  elasticsearch.keystore");
    }

    public static void main(String[] args) throws Exception {
        exit(new GenerateElasticPasswordHash().main(args, Terminal.DEFAULT));
    }

    @Override protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final Hasher hasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(env.settings()));
        try (
            SecureString elasticPassword = new SecureString(generatePassword(20));
            // There is currently no way to set the password of the elasticsearch keystore during package installation. This class
            // is called by the package installer only on installation (not on upgrades). As such we can be certain
            // that the elasticsearch keystore is obfuscated and not password protected.
            KeyStoreWrapper nodeKeystore=KeyStoreWrapper.bootstrap(env.configFile(),() -> new SecureString("")))
        {
            nodeKeystore.setString("autoconfiguration.password_hash", hasher.hash(elasticPassword));
            nodeKeystore.save(env.configFile(), new char[0]);
            terminal.println(elasticPassword.toString());
        } catch (Exception e){
            // Write nothing to stdout, so that the caller knows we failed to generate or set the password in the keystore
        }
    }
}
