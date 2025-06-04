/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.opensaml.core.config.InitializationService;
import org.opensaml.xmlsec.signature.impl.X509CertificateBuilder;
import org.slf4j.LoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SamlInit {

    private static final AtomicBoolean INITIALISED = new AtomicBoolean(false);
    private static final Logger LOGGER = LogManager.getLogger(SamlInit.class);

    private SamlInit() {}

    /**
     * This is needed in order to initialize the underlying OpenSAML library.
     * It must be called before doing anything that potentially interacts with OpenSAML (whether in server code, or in tests).
     * The initialization happens within do privileged block as the underlying Apache XML security library has a permission check.
     * The initialization happens with a specific context classloader as OpenSAML loads resources from its jar file.
     */
    public static void initialize() {
        if (INITIALISED.compareAndSet(false, true)) {
            // We want to force these classes to be loaded _before_ we fiddle with the context classloader
            LoggerFactory.getLogger(InitializationService.class);
            SpecialPermission.check();
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    LOGGER.debug("Initializing OpenSAML");
                    try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(InitializationService.class)) {
                        InitializationService.initialize();
                        // Force load this now, because it has a static field that needs to run inside the doPrivileged block
                        var ignore2 = new X509CertificateBuilder().buildObject();
                    }
                    LOGGER.debug("Initialized OpenSAML");
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw new ElasticsearchSecurityException("failed to set context classloader for SAML IdP", e);
            }
        }
    }

}
