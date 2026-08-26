/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.PullOrBuildImage;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.test.fixtures.ResourceUtils.copyResourceToFile;

public final class IdpTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "docker.elastic.co/elasticsearch-dev/idp-fixture:1.2";
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Path certsPath;

    /**
     * for packer caching only
     * */
    protected IdpTestContainer() {
        this(Network.newNetwork());
    }

    public IdpTestContainer(Network network) {
        super(new PullOrBuildImage(DOCKER_BASE_IMAGE, createFallbackImage()));
        withNetworkAliases("idp");
        withNetwork(network);
        waitingFor(Wait.forListeningPorts(4443));
        addExposedPorts(4443, 8443);
    }

    private static ImageFromDockerfile createFallbackImage() {
        return new ImageFromDockerfile("es-idp-fixture")
            // Dockerfile
            .withFileFromClasspath("Dockerfile", "/idp/Dockerfile")
            // bin directory
            .withFileFromClasspath("bin/run-jetty.sh", "/idp/bin/run-jetty.sh")
            .withFileFromClasspath("bin/init-idp.sh", "/idp/bin/init-idp.sh")
            // jetty-custom directory
            .withFileFromClasspath("jetty-custom/ssl.mod", "/idp/jetty-custom/ssl.mod")
            .withFileFromClasspath("jetty-custom/keystore", "/idp/jetty-custom/keystore")
            // shib-jetty-base directory
            .withFileFromClasspath("shib-jetty-base/start.ini", "/idp/shib-jetty-base/start.ini")
            .withFileFromClasspath("shib-jetty-base/modules/backchannel.mod", "/idp/shib-jetty-base/modules/backchannel.mod")
            .withFileFromClasspath("shib-jetty-base/webapps/idp.xml", "/idp/shib-jetty-base/webapps/idp.xml")
            .withFileFromClasspath("shib-jetty-base/start.d/ssl.ini", "/idp/shib-jetty-base/start.d/ssl.ini")
            .withFileFromClasspath("shib-jetty-base/start.d/backchannel.ini", "/idp/shib-jetty-base/start.d/backchannel.ini")
            .withFileFromClasspath("shib-jetty-base/resources/logback.xml", "/idp/shib-jetty-base/resources/logback.xml")
            .withFileFromClasspath("shib-jetty-base/resources/logback-access.xml", "/idp/shib-jetty-base/resources/logback-access.xml")
            .withFileFromClasspath("shib-jetty-base/etc/jetty-requestlog.xml", "/idp/shib-jetty-base/etc/jetty-requestlog.xml")
            .withFileFromClasspath("shib-jetty-base/etc/jetty-ssl-context.xml", "/idp/shib-jetty-base/etc/jetty-ssl-context.xml")
            .withFileFromClasspath("shib-jetty-base/etc/jetty-rewrite.xml", "/idp/shib-jetty-base/etc/jetty-rewrite.xml")
            .withFileFromClasspath("shib-jetty-base/etc/jetty-logging.xml", "/idp/shib-jetty-base/etc/jetty-logging.xml")
            .withFileFromClasspath("shib-jetty-base/etc/jetty-backchannel.xml", "/idp/shib-jetty-base/etc/jetty-backchannel.xml")
            // shibboleth-idp credentials
            .withFileFromClasspath("shibboleth-idp/credentials/idp-signing.key", "/idp/shibboleth-idp/credentials/idp-signing.key")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-signing.crt", "/idp/shibboleth-idp/credentials/idp-signing.crt")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-encryption.key", "/idp/shibboleth-idp/credentials/idp-encryption.key")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-encryption.crt", "/idp/shibboleth-idp/credentials/idp-encryption.crt")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-backchannel.p12", "/idp/shibboleth-idp/credentials/idp-backchannel.p12")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-backchannel.crt", "/idp/shibboleth-idp/credentials/idp-backchannel.crt")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-browser.p12", "/idp/shibboleth-idp/credentials/idp-browser.p12")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-browser.pem", "/idp/shibboleth-idp/credentials/idp-browser.pem")
            .withFileFromClasspath("shibboleth-idp/credentials/idp-browser.key", "/idp/shibboleth-idp/credentials/idp-browser.key")
            .withFileFromClasspath("shibboleth-idp/credentials/sealer.jks", "/idp/shibboleth-idp/credentials/sealer.jks")
            .withFileFromClasspath("shibboleth-idp/credentials/sealer.kver", "/idp/shibboleth-idp/credentials/sealer.kver")
            .withFileFromClasspath("shibboleth-idp/credentials/sp-signing.key", "/idp/shibboleth-idp/credentials/sp-signing.key")
            .withFileFromClasspath("shibboleth-idp/credentials/sp-signing.crt", "/idp/shibboleth-idp/credentials/sp-signing.crt")
            .withFileFromClasspath("shibboleth-idp/credentials/ca_server.pem", "/idp/shibboleth-idp/credentials/ca_server.pem")
            .withFileFromClasspath("shibboleth-idp/credentials/README", "/idp/shibboleth-idp/credentials/README")
            // shibboleth-idp metadata
            .withFileFromClasspath("shibboleth-idp/metadata/idp-metadata.xml", "/idp/shibboleth-idp/metadata/idp-metadata.xml")
            .withFileFromClasspath("shibboleth-idp/metadata/idp-docs-metadata.xml", "/idp/shibboleth-idp/metadata/idp-docs-metadata.xml")
            .withFileFromClasspath("shibboleth-idp/metadata/sp-metadata.xml", "/idp/shibboleth-idp/metadata/sp-metadata.xml")
            .withFileFromClasspath("shibboleth-idp/metadata/sp-metadata2.xml", "/idp/shibboleth-idp/metadata/sp-metadata2.xml")
            .withFileFromClasspath("shibboleth-idp/metadata/sp-metadata3.xml", "/idp/shibboleth-idp/metadata/sp-metadata3.xml")
            .withFileFromClasspath("shibboleth-idp/metadata/README.asciidoc", "/idp/shibboleth-idp/metadata/README.asciidoc")
            // shibboleth-idp conf
            .withFileFromClasspath("shibboleth-idp/conf/idp.properties", "/idp/shibboleth-idp/conf/idp.properties")
            .withFileFromClasspath("shibboleth-idp/conf/ldap.properties", "/idp/shibboleth-idp/conf/ldap.properties")
            .withFileFromClasspath("shibboleth-idp/conf/logback.xml", "/idp/shibboleth-idp/conf/logback.xml")
            .withFileFromClasspath("shibboleth-idp/conf/services.xml", "/idp/shibboleth-idp/conf/services.xml")
            .withFileFromClasspath("shibboleth-idp/conf/services.properties", "/idp/shibboleth-idp/conf/services.properties")
            .withFileFromClasspath("shibboleth-idp/conf/relying-party.xml", "/idp/shibboleth-idp/conf/relying-party.xml")
            .withFileFromClasspath("shibboleth-idp/conf/metadata-providers.xml", "/idp/shibboleth-idp/conf/metadata-providers.xml")
            .withFileFromClasspath("shibboleth-idp/conf/attribute-resolver.xml", "/idp/shibboleth-idp/conf/attribute-resolver.xml")
            .withFileFromClasspath("shibboleth-idp/conf/attribute-filter.xml", "/idp/shibboleth-idp/conf/attribute-filter.xml")
            .withFileFromClasspath("shibboleth-idp/conf/credentials.xml", "/idp/shibboleth-idp/conf/credentials.xml")
            .withFileFromClasspath("shibboleth-idp/conf/access-control.xml", "/idp/shibboleth-idp/conf/access-control.xml")
            .withFileFromClasspath("shibboleth-idp/conf/audit.xml", "/idp/shibboleth-idp/conf/audit.xml")
            .withFileFromClasspath("shibboleth-idp/conf/errors.xml", "/idp/shibboleth-idp/conf/errors.xml")
            .withFileFromClasspath("shibboleth-idp/conf/global.xml", "/idp/shibboleth-idp/conf/global.xml")
            .withFileFromClasspath("shibboleth-idp/conf/saml-nameid.xml", "/idp/shibboleth-idp/conf/saml-nameid.xml")
            .withFileFromClasspath("shibboleth-idp/conf/saml-nameid.properties", "/idp/shibboleth-idp/conf/saml-nameid.properties")
            .withFileFromClasspath("shibboleth-idp/conf/cas-protocol.xml", "/idp/shibboleth-idp/conf/cas-protocol.xml")
            .withFileFromClasspath("shibboleth-idp/conf/session-manager.xml", "/idp/shibboleth-idp/conf/session-manager.xml")
            // shibboleth-idp conf/authn
            .withFileFromClasspath("shibboleth-idp/conf/authn/general-authn.xml", "/idp/shibboleth-idp/conf/authn/general-authn.xml")
            .withFileFromClasspath("shibboleth-idp/conf/authn/authn-comparison.xml", "/idp/shibboleth-idp/conf/authn/authn-comparison.xml")
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/authn-events-flow.xml",
                "/idp/shibboleth-idp/conf/authn/authn-events-flow.xml"
            )
            .withFileFromClasspath("shibboleth-idp/conf/authn/duo-authn-config.xml", "/idp/shibboleth-idp/conf/authn/duo-authn-config.xml")
            .withFileFromClasspath("shibboleth-idp/conf/authn/duo.properties", "/idp/shibboleth-idp/conf/authn/duo.properties")
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/external-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/external-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/function-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/function-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/ipaddress-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/ipaddress-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/jaas-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/jaas-authn-config.xml"
            )
            .withFileFromClasspath("shibboleth-idp/conf/authn/jaas.config", "/idp/shibboleth-idp/conf/authn/jaas.config")
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/krb5-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/krb5-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/ldap-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/ldap-authn-config.xml"
            )
            .withFileFromClasspath("shibboleth-idp/conf/authn/mfa-authn-config.xml", "/idp/shibboleth-idp/conf/authn/mfa-authn-config.xml")
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/password-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/password-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/remoteuser-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/remoteuser-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/remoteuser-internal-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/remoteuser-internal-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/spnego-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/spnego-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/x509-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/x509-authn-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/authn/x509-internal-authn-config.xml",
                "/idp/shibboleth-idp/conf/authn/x509-internal-authn-config.xml"
            )
            // shibboleth-idp conf/c14n
            .withFileFromClasspath("shibboleth-idp/conf/c14n/subject-c14n.xml", "/idp/shibboleth-idp/conf/c14n/subject-c14n.xml")
            .withFileFromClasspath(
                "shibboleth-idp/conf/c14n/subject-c14n-events-flow.xml",
                "/idp/shibboleth-idp/conf/c14n/subject-c14n-events-flow.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/c14n/simple-subject-c14n-config.xml",
                "/idp/shibboleth-idp/conf/c14n/simple-subject-c14n-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/c14n/x500-subject-c14n-config.xml",
                "/idp/shibboleth-idp/conf/c14n/x500-subject-c14n-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/c14n/attribute-sourced-subject-c14n-config.xml",
                "/idp/shibboleth-idp/conf/c14n/attribute-sourced-subject-c14n-config.xml"
            )
            // shibboleth-idp conf/intercept
            .withFileFromClasspath(
                "shibboleth-idp/conf/intercept/profile-intercept.xml",
                "/idp/shibboleth-idp/conf/intercept/profile-intercept.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/intercept/intercept-events-flow.xml",
                "/idp/shibboleth-idp/conf/intercept/intercept-events-flow.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/intercept/consent-intercept-config.xml",
                "/idp/shibboleth-idp/conf/intercept/consent-intercept-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/intercept/context-check-intercept-config.xml",
                "/idp/shibboleth-idp/conf/intercept/context-check-intercept-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/intercept/expiring-password-intercept-config.xml",
                "/idp/shibboleth-idp/conf/intercept/expiring-password-intercept-config.xml"
            )
            .withFileFromClasspath(
                "shibboleth-idp/conf/intercept/impersonate-intercept-config.xml",
                "/idp/shibboleth-idp/conf/intercept/impersonate-intercept-config.xml"
            )
            // shibboleth-idp conf/admin
            .withFileFromClasspath("shibboleth-idp/conf/admin/general-admin.xml", "/idp/shibboleth-idp/conf/admin/general-admin.xml")
            .withFileFromClasspath("shibboleth-idp/conf/admin/metrics.xml", "/idp/shibboleth-idp/conf/admin/metrics.xml")
            // shibboleth-idp views
            .withFileFromClasspath("shibboleth-idp/views/login.vm", "/idp/shibboleth-idp/views/login.vm")
            .withFileFromClasspath("shibboleth-idp/views/login-error.vm", "/idp/shibboleth-idp/views/login-error.vm")
            .withFileFromClasspath("shibboleth-idp/views/logout.vm", "/idp/shibboleth-idp/views/logout.vm")
            .withFileFromClasspath("shibboleth-idp/views/logout-complete.vm", "/idp/shibboleth-idp/views/logout-complete.vm")
            .withFileFromClasspath("shibboleth-idp/views/logout-propagate.vm", "/idp/shibboleth-idp/views/logout-propagate.vm")
            .withFileFromClasspath("shibboleth-idp/views/error.vm", "/idp/shibboleth-idp/views/error.vm")
            .withFileFromClasspath("shibboleth-idp/views/duo.vm", "/idp/shibboleth-idp/views/duo.vm")
            .withFileFromClasspath("shibboleth-idp/views/spnego-unavailable.vm", "/idp/shibboleth-idp/views/spnego-unavailable.vm")
            .withFileFromClasspath("shibboleth-idp/views/user-prefs.vm", "/idp/shibboleth-idp/views/user-prefs.vm")
            .withFileFromClasspath("shibboleth-idp/views/admin/unlock-keys.vm", "/idp/shibboleth-idp/views/admin/unlock-keys.vm")
            .withFileFromClasspath(
                "shibboleth-idp/views/intercept/attribute-release.vm",
                "/idp/shibboleth-idp/views/intercept/attribute-release.vm"
            )
            .withFileFromClasspath(
                "shibboleth-idp/views/intercept/expiring-password.vm",
                "/idp/shibboleth-idp/views/intercept/expiring-password.vm"
            )
            .withFileFromClasspath("shibboleth-idp/views/intercept/impersonate.vm", "/idp/shibboleth-idp/views/intercept/impersonate.vm")
            .withFileFromClasspath("shibboleth-idp/views/intercept/terms-of-use.vm", "/idp/shibboleth-idp/views/intercept/terms-of-use.vm")
            .withFileFromClasspath(
                "shibboleth-idp/views/client-storage/client-storage-read.vm",
                "/idp/shibboleth-idp/views/client-storage/client-storage-read.vm"
            )
            .withFileFromClasspath(
                "shibboleth-idp/views/client-storage/client-storage-write.vm",
                "/idp/shibboleth-idp/views/client-storage/client-storage-write.vm"
            )
            // shibboleth-idp webapp
            .withFileFromClasspath("shibboleth-idp/webapp/css/consent.css", "/idp/shibboleth-idp/webapp/css/consent.css")
            .withFileFromClasspath("shibboleth-idp/webapp/css/logout.css", "/idp/shibboleth-idp/webapp/css/logout.css")
            .withFileFromClasspath("shibboleth-idp/webapp/css/main.css", "/idp/shibboleth-idp/webapp/css/main.css")
            .withFileFromClasspath("shibboleth-idp/webapp/images/dummylogo.png", "/idp/shibboleth-idp/webapp/images/dummylogo.png")
            .withFileFromClasspath(
                "shibboleth-idp/webapp/images/dummylogo-mobile.png",
                "/idp/shibboleth-idp/webapp/images/dummylogo-mobile.png"
            )
            .withFileFromClasspath("shibboleth-idp/webapp/images/failure-32x32.png", "/idp/shibboleth-idp/webapp/images/failure-32x32.png")
            .withFileFromClasspath("shibboleth-idp/webapp/images/success-32x32.png", "/idp/shibboleth-idp/webapp/images/success-32x32.png");
    }

    @Override
    public void stop() {
        super.stop();
        temporaryFolder.delete();
    }

    public Path getBrowserPem() {
        try {
            temporaryFolder.create();
            certsPath = temporaryFolder.newFolder("certs").toPath();
            return copyResourceToFile(getClass(), certsPath, "idp/shibboleth-idp/credentials/idp-browser.pem");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Integer getDefaultPort() {
        return getMappedPort(4443);
    }
}
