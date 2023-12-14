/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.test.fixtures.ResourceUtils.copyResourceToFile;

public final class IdpTestContainer extends DockerEnvironmentAwareTestContainer {

    public static final String DOCKER_BASE_IMAGE = "openjdk:11.0.16-jre";

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Path certsPath;

    /**
     * for packer caching only
     * */
    protected IdpTestContainer() {
        this(Network.newNetwork());
    }

    public IdpTestContainer(Network network) {
        super(
            new ImageFromDockerfile("es-idp-testfixture").withDockerfileFromBuilder(
                builder -> builder.from(DOCKER_BASE_IMAGE)
                    .env("jetty_version", "9.3.27.v20190418")
                    .env("jetty_hash", "7c7c80dd1c9f921771e2b1a05deeeec652d5fcaa")
                    .env("idp_version", "3.4.3")
                    .env("idp_hash", "eb86bc7b6366ce2a44f97cae1b014d307b84257e3149469b22b2d091007309db")
                    .env("dta_hash", "2f547074b06952b94c35631398f36746820a7697")
                    .env("slf4j_version", "1.7.25")
                    .env("slf4j_hash", "da76ca59f6a57ee3102f8f9bd9cee742973efa8a")
                    .env("logback_version", "1.2.3")
                    .env("logback_classic_hash", "7c4f3c474fb2c041d8028740440937705ebb473a")
                    .env("logback_core_hash", "864344400c3d4d92dfeb0a305dc87d953677c03c")
                    .env("logback_access_hash", "e8a841cb796f6423c7afd8738df6e0e4052bf24a")

                    .env("JETTY_HOME", "/opt/jetty-home")
                    .env("JETTY_BASE", "/opt/shib-jetty-base")
                    .env("PATH", "$PATH:$JAVA_HOME/bin")
                    .env("JETTY_BROWSER_SSL_KEYSTORE_PASSWORD", "secret")
                    .env("JETTY_BACKCHANNEL_SSL_KEYSTORE_PASSWORD", "secret")
                    .env("JETTY_MAX_HEAP", "64m")
                    // Manually override the jetty keystore otherwise it will attempt to download and fail
                    .run("mkdir -p /opt/shib-jetty-base/modules")
                    .copy("idp/jetty-custom/ssl.mod", "/opt/shib-jetty-base/modules/ssl.mod")
                    .copy("idp/jetty-custom/keystore", "/opt/shib-jetty-base/etc/keystore")
                    // Download Jetty, verify the hash, and install, initialize a new base
                    .run(
                        "wget -q https://repo.maven.apache.org/maven2/org/eclipse/jetty/jetty-distribution/$jetty_version/jetty-distribution-$jetty_version.tar.gz"
                            + " && echo \"$jetty_hash  jetty-distribution-$jetty_version.tar.gz\" | sha1sum -c -"
                            + " && tar -zxvf jetty-distribution-$jetty_version.tar.gz -C /opt"
                            + " && ln -s /opt/jetty-distribution-$jetty_version/ /opt/jetty-home"
                    )
                    // Config Jetty
                    .run(
                        "mkdir -p /opt/shib-jetty-base/modules /opt/shib-jetty-base/lib/ext  /opt/shib-jetty-base/lib/logging /opt/shib-jetty-base/resources"
                            + " && cd /opt/shib-jetty-base"
                            + " && touch start.ini"
                            + " && java -jar ../jetty-home/start.jar --add-to-startd=http,https,deploy,ext,annotations,jstl,rewrite"
                    )
                    // Download Shibboleth IdP, verify the hash, and install
                    .run(
                        "wget -q https://shibboleth.net/downloads/identity-provider/archive/$idp_version/shibboleth-identity-provider-$idp_version.tar.gz"
                            + " && echo \"$idp_hash  shibboleth-identity-provider-$idp_version.tar.gz\" | sha256sum -c -"
                            + " && tar -zxvf  shibboleth-identity-provider-$idp_version.tar.gz -C /opt"
                            + " && ln -s /opt/shibboleth-identity-provider-$idp_version/ /opt/shibboleth-idp"
                    )
                    // Download the library to allow SOAP Endpoints, verify the hash, and place
                    .run(
                        "wget -q https://build.shibboleth.net/nexus/content/repositories/releases/net/shibboleth/utilities/jetty9/jetty9-dta-ssl/1.0.0/jetty9-dta-ssl-1.0.0.jar"
                            + " && echo \"$dta_hash jetty9-dta-ssl-1.0.0.jar\" | sha1sum -c -"
                            + " && mv jetty9-dta-ssl-1.0.0.jar /opt/shib-jetty-base/lib/ext/"
                    )
                    // Download the slf4j library for Jetty logging, verify the hash, and place
                    .run(
                        "wget -q https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/$slf4j_version/slf4j-api-$slf4j_version.jar"
                            + " && echo \"$slf4j_hash  slf4j-api-$slf4j_version.jar\" | sha1sum -c -"
                            + " && mv slf4j-api-$slf4j_version.jar /opt/shib-jetty-base/lib/logging/"
                    )
                    // Download the logback_classic library for Jetty logging, verify the hash, and place
                    .run(
                        "wget -q https://repo.maven.apache.org/maven2/ch/qos/logback/logback-classic/$logback_version/logback-classic-$logback_version.jar"
                            + " && echo \"$logback_classic_hash  logback-classic-$logback_version.jar\" | sha1sum -c -"
                            + " && mv logback-classic-$logback_version.jar /opt/shib-jetty-base/lib/logging/"
                    )
                    // Download the logback-core library for Jetty logging, verify the hash, and place
                    .run(
                        "wget -q https://repo.maven.apache.org/maven2/ch/qos/logback/logback-core/$logback_version/logback-core-$logback_version.jar"
                            + " && echo \"$logback_core_hash  logback-core-$logback_version.jar\" | sha1sum -c -"
                            + " && mv logback-core-$logback_version.jar /opt/shib-jetty-base/lib/logging/"
                    )
                    // Download the logback-access library for Jetty logging, verify the hash, and place
                    .run(
                        "wget -q https://repo.maven.apache.org/maven2/ch/qos/logback/logback-access/$logback_version/logback-access-$logback_version.jar"
                            + " && echo \"$logback_access_hash  logback-access-$logback_version.jar\" | sha1sum -c -"
                            + " && mv logback-access-$logback_version.jar /opt/shib-jetty-base/lib/logging/"
                    )
                    // ## Copy local files
                    .copy("idp/shib-jetty-base/", "/opt/shib-jetty-base/")
                    .copy("idp/shibboleth-idp/", "/opt/shibboleth-idp/")
                    .copy("idp/bin/", "/usr/local/bin/")
                    // Setting owner ownership and permissions
                    .run(
                        "useradd jetty -U -s /bin/false"
                            + " && chown -R root:jetty /opt"
                            + " && chmod -R 640 /opt"
                            + " && chown -R root:jetty /opt/shib-jetty-base"
                            + " && chmod -R 640 /opt/shib-jetty-base"
                            + " && chmod -R 750 /opt/shibboleth-idp/bin"
                    )
                    .run("chmod 750 /usr/local/bin/run-jetty.sh /usr/local/bin/init-idp.sh")
                    .run("chmod +x /opt/jetty-home/bin/jetty.sh")
                    // Opening 4443 (browser TLS), 8443 (mutual auth TLS)
                    .cmd("run-jetty.sh")
                    // .expose(4443)
                    .build()

            )
                .withFileFromClasspath("idp/jetty-custom/ssl.mod", "/idp/jetty-custom/ssl.mod")
                .withFileFromClasspath("idp/jetty-custom/keystore", "/idp/jetty-custom/keystore")
                .withFileFromClasspath("idp/shib-jetty-base/", "/idp/shib-jetty-base/")
                .withFileFromClasspath("idp/shibboleth-idp/", "/idp/shibboleth-idp/")
                .withFileFromClasspath("idp/bin/", "/idp/bin/")

        );
        withNetworkAliases("idp");
        withNetwork(network);
        addExposedPorts(4443, 8443);
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
