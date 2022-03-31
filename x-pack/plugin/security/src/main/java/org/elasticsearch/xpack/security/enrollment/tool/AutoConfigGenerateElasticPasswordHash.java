/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import joptsimple.OptionSet;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.KeyStoreAwareCommand;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_ELASTIC_PASSWORD_HASH;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

/**
 * This tool is not meant to be used in user facing CLI tools. It is called by the package installers only upon installation. It
 * <ul>
 *     <li>generates a random strong password for the elastic user/li>
 *     <li>stores a salted hash of that password in the elasticsearch keystore, in the autoconfiguration.password_hash setting</li>
 * </ul>
 * This password is subsequently picked up by the node on startup and is set as the password of the elastic user in the security index.
 *
 * There is currently no way to set the password of the elasticsearch keystore during package installation. This tool
 * is called by the package installer only on installation (not on upgrades) so we can be certain that the keystore
 * has an empty password (obfuscated).
 *
 * The generated password is written to stdout upon success. Error messages are printed to stderr.
 */
public class AutoConfigGenerateElasticPasswordHash extends KeyStoreAwareCommand {

    public AutoConfigGenerateElasticPasswordHash() {
        super("Generates a password hash for for the elastic user and stores it in elasticsearch.keystore");
    }

    public static void main(String[] args) throws Exception {
        exit(new AutoConfigGenerateElasticPasswordHash().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final Hasher hasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(env.settings()));
        try (
            SecureString elasticPassword = new SecureString(generatePassword(20));
            KeyStoreWrapper nodeKeystore = KeyStoreWrapper.bootstrap(env.configFile(), () -> new SecureString(new char[0]))
        ) {
            nodeKeystore.setString(AUTOCONFIG_ELASTIC_PASSWORD_HASH.getKey(), hasher.hash(elasticPassword));
            nodeKeystore.save(env.configFile(), new char[0]);
            terminal.print(Terminal.Verbosity.NORMAL, elasticPassword.toString());
        } catch (Exception e) {
            throw new UserException(ExitCodes.CANT_CREATE, "Failed to generate a password for the elastic user", e);
        }
    }
}
