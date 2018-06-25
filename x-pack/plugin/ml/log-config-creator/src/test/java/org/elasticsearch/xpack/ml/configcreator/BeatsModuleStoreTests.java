/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.xpack.ml.configcreator.BeatsModuleStore.BeatsModule;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class BeatsModuleStoreTests extends LogConfigCreatorTestCase {

    private static final String APACHE2_ACCESS_LOG_SAMPLE = "123.4.51.181 - - [24/Apr/2010:09:56:26 -0700] " +
        "\"GET http://proxyjudge1.proxyfire.net/fastenv HTTP/1.1\" 404 1466 \"-\" " +
        "\"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\" bK4J0AoAAQ4AAEHHC2QAAAAE 143752";

    private static final String APACHE2_ACCESS_MANIFEST = "module_version: 1.0\n" +
        "\n" +
        "var:\n" +
        "  - name: paths\n" +
        "    default:\n" +
        "      - /var/log/apache2/access.log*\n" +
        "      - /var/log/apache2/other_vhosts_access.log*\n" +
        "    os.darwin:\n" +
        "      - /usr/local/var/log/apache2/access_log*\n" +
        "    os.windows:\n" +
        "      - \"C:/tools/Apache/httpd-2.*/Apache24/logs/access.log*\"\n" +
        "      - \"C:/Program Files/Apache Software Foundation/Apache2.*/logs/access.log*\"\n" +
        "\n" +
        "ingest_pipeline: ingest/default.json\n" +
        "input: config/access.yml\n" +
        "\n" +
        "requires.processors:\n" +
        "- name: user_agent\n" +
        "  plugin: ingest-user-agent\n" +
        "- name: geoip\n" +
        "  plugin: ingest-geoip\n";
    private static final String APACHE2_ACCESS_CONFIG = "type: log\n" +
        "paths:\n" +
        "{{ range $i, $path := .paths }}\n" +
        " - {{$path}}\n" +
        "{{ end }}\n" +
        "exclude_files: [\".gz$\"]\n";
    private static final String APACHE2_ACCESS_PIPELINE = "{\n" +
        "  \"description\": \"Pipeline for parsing Apache2 access logs. Requires the geoip and user_agent plugins.\",\n" +
        "  \"processors\": [{\n" +
        "    \"grok\": {\n" +
        "      \"field\": \"message\",\n" +
        "      \"patterns\":[\n" +
        "        \"%{IPORHOST:apache2.access.remote_ip} - %{DATA:apache2.access.user_name} \\\\[%{HTTPDATE:apache2.access.time}\\\\] " +
            "\\\"%{WORD:apache2.access.method} %{DATA:apache2.access.url} HTTP/%{NUMBER:apache2.access.http_version}\\\" " +
            "%{NUMBER:apache2.access.response_code} (?:%{NUMBER:apache2.access.body_sent.bytes}|-)" +
            "( \\\"%{DATA:apache2.access.referrer}\\\")?( \\\"%{DATA:apache2.access.agent}\\\")?\",\n" +
        "        \"%{IPORHOST:apache2.access.remote_ip} - %{DATA:apache2.access.user_name} \\\\[%{HTTPDATE:apache2.access.time}\\\\] " +
            "\\\"-\\\" %{NUMBER:apache2.access.response_code} -\"\n" +
        "        ],\n" +
        "      \"ignore_missing\": true\n" +
        "    }\n" +
        "  },{\n" +
        "    \"remove\":{\n" +
        "      \"field\": \"message\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"rename\": {\n" +
        "      \"field\": \"@timestamp\",\n" +
        "      \"target_field\": \"read_timestamp\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"date\": {\n" +
        "      \"field\": \"apache2.access.time\",\n" +
        "      \"target_field\": \"@timestamp\",\n" +
        "      \"formats\": [\"dd/MMM/YYYY:H:m:s Z\"]\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"remove\": {\n" +
        "      \"field\": \"apache2.access.time\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"user_agent\": {\n" +
        "      \"field\": \"apache2.access.agent\",\n" +
        "      \"target_field\": \"apache2.access.user_agent\",\n" +
        "      \"ignore_failure\": true\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"remove\": {\n" +
        "      \"field\": \"apache2.access.agent\",\n" +
        "      \"ignore_failure\": true\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"geoip\": {\n" +
        "      \"field\": \"apache2.access.remote_ip\",\n" +
        "      \"target_field\": \"apache2.access.geoip\"\n" +
        "    }\n" +
        "  }],\n" +
        "  \"on_failure\" : [{\n" +
        "    \"set\" : {\n" +
        "      \"field\" : \"error.message\",\n" +
        "      \"value\" : \"{{ _ingest.on_failure_message }}\"\n" +
        "    }\n" +
        "  }]\n" +
        "}\n";
    private static final String APACHE2_ERROR_MANIFEST = "module_version: 1.0\n" +
        "\n" +
        "var:\n" +
        "  - name: paths\n" +
        "    default:\n" +
        "      - /var/log/apache2/error.log*\n" +
        "    os.darwin:\n" +
        "      - /usr/local/var/log/apache2/error_log*\n" +
        "    os.windows:\n" +
        "      - \"C:/tools/Apache/httpd-2.*/Apache24/logs/error.log*\"\n" +
        "      - \"C:/Program Files/Apache Software Foundation/Apache2.*/logs/error.log*\"\n" +
        "\n" +
        "ingest_pipeline: ingest/pipeline.json\n" +
        "input: config/error.yml\n";
    private static final String APACHE2_ERROR_CONFIG = "type: log\n" +
        "paths:\n" +
        "{{ range $i, $path := .paths }}\n" +
        " - {{$path}}\n" +
        "{{ end }}\n" +
        "exclude_files: [\".gz$\"]\n";
    private static final String APACHE2_ERROR_PIPELINE = "{\n" +
        "  \"description\": \"Pipeline for parsing apache2 error logs\",\n" +
        "  \"processors\": [\n" +
        "    {\n" +
        "      \"grok\": {\n" +
        "        \"field\": \"message\",\n" +
        "        \"patterns\": [\n" +
        "          \"\\\\[%{APACHE_TIME:apache2.error.timestamp}\\\\] \\\\[%{LOGLEVEL:apache2.error.level}\\\\]" +
            "( \\\\[client %{IPORHOST:apache2.error.client}\\\\])? %{GREEDYDATA:apache2.error.message}\",\n" +
        "          \"\\\\[%{APACHE_TIME:apache2.error.timestamp}\\\\] " +
            "\\\\[%{DATA:apache2.error.module}:%{LOGLEVEL:apache2.error.level}\\\\] " +
            "\\\\[pid %{NUMBER:apache2.error.pid}(:tid %{NUMBER:apache2.error.tid})?\\\\]" +
            "( \\\\[client %{IPORHOST:apache2.error.client}\\\\])? %{GREEDYDATA:apache2.error.message1}\"\n" +
        "        ],\n" +
        "        \"pattern_definitions\": {\n" +
        "          \"APACHE_TIME\": \"%{DAY} %{MONTH} %{MONTHDAY} %{TIME} %{YEAR}\"\n" +
        "        },\n" +
        "        \"ignore_missing\": true\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"remove\":{\n" +
        "        \"field\": \"message\"\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"rename\": {\n" +
        "        \"field\": \"apache2.error.message1\",\n" +
        "        \"target_field\": \"apache2.error.message\",\n" +
        "        \"ignore_failure\": true\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"date\": {\n" +
        "        \"field\": \"apache2.error.timestamp\",\n" +
        "        \"target_field\": \"@timestamp\",\n" +
        "        \"formats\": [\"EEE MMM dd H:m:s YYYY\", \"EEE MMM dd H:m:s.SSSSSS YYYY\"],\n" +
        "        \"ignore_failure\": true\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"remove\": {\n" +
        "        \"field\": \"apache2.error.timestamp\",\n" +
        "        \"ignore_failure\": true\n" +
        "      }\n" +
        "    }\n" +
        "  ],\n" +
        "  \"on_failure\" : [{\n" +
        "    \"set\" : {\n" +
        "      \"field\" : \"error.message\",\n" +
        "      \"value\" : \"{{ _ingest.on_failure_message }}\"\n" +
        "    }\n" +
        "  }]\n" +
        "}\n";

    private static final String SYSTEM_AUTH_MANIFEST = "module_version: 1.0\n" +
        "\n" +
        "var:\n" +
        "  - name: paths\n" +
        "    default:\n" +
        "      - /var/log/auth.log*\n" +
        "      - /var/log/secure*\n" +
        "    os.darwin:\n" +
        "      # this works in OS X < 10.8. Newer darwin versions don't write\n" +
        "      # ssh logs to files\n" +
        "      - /var/log/secure.log*\n" +
        "    os.windows: []\n" +
        "  - name: convert_timezone\n" +
        "    default: false\n" +
        "    # if ES < 6.1.0, this flag switches to false automatically when evaluating the\n" +
        "    # pipeline\n" +
        "    min_elasticsearch_version:\n" +
        "      version: 6.1.0\n" +
        "      value: false\n" +
        "\n" +
        "ingest_pipeline: ingest/pipeline.json\n" +
        "input: config/auth.yml\n";
    private static final String SYSTEM_AUTH_CONFIG = "type: log\n" +
        "paths:\n" +
        "{{ range $i, $path := .paths }}\n" +
        " - {{$path}}\n" +
        "{{ end }}\n" +
        "exclude_files: [\".gz$\"]\n" +
        "multiline:\n" +
        "  pattern: \"^\\\\s\"\n" +
        "  match: after\n" +
        "{{ if .convert_timezone }}\n" +
        "processors:\n" +
        "- add_locale: ~\n" +
        "{{ end }}\n";
    private static final String SYSTEM_AUTH_PIPELINE = "{\n" +
        "  \"description\": \"Pipeline for parsing system authorisation/secure logs\",\n" +
        "  \"processors\": [\n" +
        "    {\n" +
        "      \"grok\": {\n" +
        "        \"field\": \"message\",\n" +
        "        \"ignore_missing\": true,\n" +
        "        \"pattern_definitions\" : {\n" +
        "          \"GREEDYMULTILINE\" : \"(.|\\n)*\"\n" +
        "        },\n" +
        "        \"patterns\": [\n" +
        "          \"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname} sshd" +
            "(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: %{DATA:system.auth.ssh.event} %{DATA:system.auth.ssh.method} for (invalid user )?" +
            "%{DATA:system.auth.user} from %{IPORHOST:system.auth.ssh.ip} port %{NUMBER:system.auth.ssh.port} " +
            "ssh2(: %{GREEDYDATA:system.auth.ssh.signature})?\",\n" +
        "          \"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname} sshd" +
            "(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: %{DATA:system.auth.ssh.event} user %{DATA:system.auth.user} from " +
            "%{IPORHOST:system.auth.ssh.ip}\",\n" +
        "          \"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname} sshd" +
            "(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: Did not receive identification string from " +
            "%{IPORHOST:system.auth.ssh.dropped_ip}\",\n" +
        "          \"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname} sudo" +
            "(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: \\\\s*%{DATA:system.auth.user} :( %{DATA:system.auth.sudo.error} ;)? " +
            "TTY=%{DATA:system.auth.sudo.tty} ; PWD=%{DATA:system.auth.sudo.pwd} ; USER=%{DATA:system.auth.sudo.user} ; " +
            "COMMAND=%{GREEDYDATA:system.auth.sudo.command}\",\n" +
        "          \"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname} groupadd" +
            "(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: new group: name=%{DATA:system.auth.groupadd.name}, " +
            "GID=%{NUMBER:system.auth.groupadd.gid}\",\n" +
        "          \"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname} useradd" +
            "(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: new user: name=%{DATA:system.auth.useradd.name}, " +
            "UID=%{NUMBER:system.auth.useradd.uid}, GID=%{NUMBER:system.auth.useradd.gid}, home=%{DATA:system.auth.useradd.home}, " +
            "shell=%{DATA:system.auth.useradd.shell}$\",\n" +
        "\t\t\t\t\t\"%{SYSLOGTIMESTAMP:system.auth.timestamp} %{SYSLOGHOST:system.auth.hostname}? " +
            "%{DATA:system.auth.program}(?:\\\\[%{POSINT:system.auth.pid}\\\\])?: %{GREEDYMULTILINE:system.auth.message}\"\n" +
        "        ]\n" +
        "      }\n" +
        "    },\n" +
        "\t\t{\n" +
        "      \"remove\": {\n" +
        "        \"field\": \"message\"\n" +
        "      }\n" +
        "    },\n" +
        "\t\t{\n" +
        "      \"date\": {\n" +
        "        \"field\": \"system.auth.timestamp\",\n" +
        "        \"target_field\": \"@timestamp\",\n" +
        "        \"formats\": [\n" +
        "\t\t\t\t\t\"MMM  d HH:mm:ss\",\n" +
        "\t\t\t\t\t\"MMM dd HH:mm:ss\"\n" +
        "        ],\n" +
        "        {< if .convert_timezone >}\"timezone\": \"{{ beat.timezone }}\",{< end >}\n" +
        "        \"ignore_failure\": true\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"geoip\": {\n" +
        "        \"field\": \"system.auth.ssh.ip\",\n" +
        "        \"target_field\": \"system.auth.ssh.geoip\",\n" +
        "        \"ignore_failure\": true\n" +
        "      }\n" +
        "    }\n" +
        "  ],\n" +
        "  \"on_failure\" : [{\n" +
        "    \"set\" : {\n" +
        "      \"field\" : \"error.message\",\n" +
        "      \"value\" : \"{{ _ingest.on_failure_message }}\"\n" +
        "    }\n" +
        "  }]\n" +
        "}\n";
    private static final String SYSTEM_SYSLOG_MANIFEST = "module_version: \"1.0\"\n" +
        "\n" +
        "var:\n" +
        "  - name: paths\n" +
        "    default:\n" +
        "      - /var/log/messages*\n" +
        "      - /var/log/syslog*\n" +
        "    os.darwin:\n" +
        "      - /var/log/system.log*\n" +
        "    os.windows: []\n" +
        "  - name: convert_timezone\n" +
        "    default: false\n" +
        "    # if ES < 6.1.0, this flag switches to false automatically when evaluating the\n" +
        "    # pipeline\n" +
        "    min_elasticsearch_version:\n" +
        "      version: 6.1.0\n" +
        "      value: false\n" +
        "\n" +
        "ingest_pipeline: ingest/pipeline.json\n" +
        "input: config/syslog.yml\n";
    private static final String SYSTEM_SYSLOG_CONFIG = "type: log\n" +
        "paths:\n" +
        "{{ range $i, $path := .paths }}\n" +
        " - {{$path}}\n" +
        "{{ end }}\n" +
        "exclude_files: [\".gz$\"]\n" +
        "multiline:\n" +
        "  pattern: \"^\\\\s\"\n" +
        "  match: after\n" +
        "{{ if .convert_timezone }}\n" +
        "processors:\n" +
        "- add_locale: ~\n" +
        "{{ end }}\n";
    private static final String SYSTEM_SYSLOG_PIPELINE = "{\n" +
        "\t\"description\": \"Pipeline for parsing Syslog messages.\",\n" +
        "\t\"processors\": [\n" +
        "\t\t{\n" +
        "\t\t\t\"grok\": {\n" +
        "\t\t\t\t\"field\": \"message\",\n" +
        "\t\t\t\t\"patterns\": [\n" +
        "\t\t\t\t\t\"%{SYSLOGTIMESTAMP:system.syslog.timestamp} %{SYSLOGHOST:system.syslog.hostname} %{DATA:system.syslog.program}" +
            "(?:\\\\[%{POSINT:system.syslog.pid}\\\\])?: %{GREEDYMULTILINE:system.syslog.message}\",\n" +
        "\t\t\t\t\t\"%{SYSLOGTIMESTAMP:system.syslog.timestamp} %{GREEDYMULTILINE:system.syslog.message}\"\n" +
        "\t\t\t\t],\n" +
        "        \"pattern_definitions\" : {\n" +
        "          \"GREEDYMULTILINE\" : \"(.|\\n)*\"\n" +
        "        },\n" +
        "        \"ignore_missing\": true\n" +
        "\t\t\t}\n" +
        "    },\n" +
        "\t\t{\n" +
        "      \"remove\": {\n" +
        "        \"field\": \"message\"\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"date\": {\n" +
        "        \"field\": \"system.syslog.timestamp\",\n" +
        "        \"target_field\": \"@timestamp\",\n" +
        "        \"formats\": [\n" +
        "\t\t\t\t\t\"MMM  d HH:mm:ss\",\n" +
        "\t\t\t\t\t\"MMM dd HH:mm:ss\"\n" +
        "        ],\n" +
        "        {< if .convert_timezone >}\"timezone\": \"{{ beat.timezone }}\",{< end >}\n" +
        "        \"ignore_failure\": true\n" +
        "      }\n" +
        "    }\n" +
        "\t],\n" +
        "  \"on_failure\" : [{\n" +
        "    \"set\" : {\n" +
        "      \"field\" : \"error.message\",\n" +
        "      \"value\" : \"{{ _ingest.on_failure_message }}\"\n" +
        "    }\n" +
        "  }]\n" +
        "}\n";

    private Path moduleDir;

    @Before
    public void setup() throws IOException {
        moduleDir = createTempDir().resolve("module");

        Path apache2Dir = moduleDir.resolve("apache2");
        Path accessDir = apache2Dir.resolve("access");
        Path accessConfigDir = accessDir.resolve("config");
        Files.createDirectories(accessConfigDir);
        Path accessIngestDir = accessDir.resolve("ingest");
        Files.createDirectories(accessIngestDir);
        Path errorDir = apache2Dir.resolve("error");
        Path errorConfigDir = errorDir.resolve("config");
        Files.createDirectories(errorConfigDir);
        Path errorIngestDir = errorDir.resolve("ingest");
        Files.createDirectories(errorIngestDir);

        Files.write(accessDir.resolve("manifest.yml"), APACHE2_ACCESS_MANIFEST.getBytes(StandardCharsets.UTF_8));
        Files.write(accessConfigDir.resolve("access.yml"), APACHE2_ACCESS_CONFIG.getBytes(StandardCharsets.UTF_8));
        Files.write(accessIngestDir.resolve("default.json"), APACHE2_ACCESS_PIPELINE.getBytes(StandardCharsets.UTF_8));
        Files.write(errorDir.resolve("manifest.yml"), APACHE2_ERROR_MANIFEST.getBytes(StandardCharsets.UTF_8));
        Files.write(errorConfigDir.resolve("error.yml"), APACHE2_ERROR_CONFIG.getBytes(StandardCharsets.UTF_8));
        Files.write(errorIngestDir.resolve("pipeline.json"), APACHE2_ERROR_PIPELINE.getBytes(StandardCharsets.UTF_8));

        Path systemDir = moduleDir.resolve("system");
        Path authDir = systemDir.resolve("auth");
        Path authConfigDir = authDir.resolve("config");
        Files.createDirectories(authConfigDir);
        Path authIngestDir = authDir.resolve("ingest");
        Files.createDirectories(authIngestDir);
        Path syslogDir = systemDir.resolve("syslog");
        Path syslogConfigDir = syslogDir.resolve("config");
        Files.createDirectories(syslogConfigDir);
        Path syslogIngestDir = syslogDir.resolve("ingest");
        Files.createDirectories(syslogIngestDir);

        Files.write(authDir.resolve("manifest.yml"), SYSTEM_AUTH_MANIFEST.getBytes(StandardCharsets.UTF_8));
        Files.write(authConfigDir.resolve("auth.yml"), SYSTEM_AUTH_CONFIG.getBytes(StandardCharsets.UTF_8));
        Files.write(authIngestDir.resolve("pipeline.json"), SYSTEM_AUTH_PIPELINE.getBytes(StandardCharsets.UTF_8));
        Files.write(syslogDir.resolve("manifest.yml"), SYSTEM_SYSLOG_MANIFEST.getBytes(StandardCharsets.UTF_8));
        Files.write(syslogConfigDir.resolve("syslog.yml"), SYSTEM_SYSLOG_CONFIG.getBytes(StandardCharsets.UTF_8));
        Files.write(syslogIngestDir.resolve("pipeline.json"), SYSTEM_SYSLOG_PIPELINE.getBytes(StandardCharsets.UTF_8));
    }

    public void testParseModule() {
        List<BeatsModule> beatsModules = new ArrayList<>();

        BeatsModuleStore.parseModule(moduleDir.resolve("apache2").resolve("access").resolve("manifest.yml"), beatsModules,
            "my_dir/access.log");

        assertEquals(1, beatsModules.size());
        BeatsModule apache2AccessModule = beatsModules.get(0);
        assertEquals("apache2", apache2AccessModule.moduleName);
        assertEquals("access", apache2AccessModule.fileType);
        assertEquals("- type: log\n" +
            "  paths:\n" +
            "   - 'my_dir/access.log'\n" +
            "  exclude_files: [\".gz$\"]", apache2AccessModule.inputDefinition);
        assertEquals(APACHE2_ACCESS_PIPELINE.trim(), apache2AccessModule.ingestPipeline);
    }

    public void testPopulateModuleData() throws IOException {

        List<BeatsModule> beatsModules = BeatsModuleStore.populateModuleData(moduleDir, "my_dir/error_log.log");

        assertEquals(1, beatsModules.size());
        Set<String> moduleNames = beatsModules.stream().map(beatsModule -> beatsModule.moduleName).collect(Collectors.toSet());
        assertThat(moduleNames, contains("apache2"));
        Set<String> fileTypes = beatsModules.stream().map(beatsModule -> beatsModule.fileType).collect(Collectors.toSet());
        assertThat(fileTypes, contains("error"));
    }

    public void testFindMatchingModuleGivenRepoAvailable() throws IOException {

        BeatsModuleStore beatsModuleStore = new BeatsModuleStore(moduleDir, "logs/www-access.log");

        BeatsModule module = beatsModuleStore.findMatchingModule(APACHE2_ACCESS_LOG_SAMPLE);
        assertNotNull(module);
        assertEquals("apache2", module.moduleName);
        assertEquals("access", module.fileType);
        assertThat(module.inputDefinition, containsString("www-access.log"));
        assertThat(module.ingestPipeline, containsString("Pipeline for parsing Apache2 access logs"));
    }

    public void testFindMatchingModuleGivenNoRepoAvailable() throws IOException {

        BeatsModuleStore beatsModuleStore = new BeatsModuleStore(null, "logs/www-access.log");

        assertNull(beatsModuleStore.findMatchingModule(APACHE2_ACCESS_LOG_SAMPLE));

        beatsModuleStore = new BeatsModuleStore(createTempDir().resolve("module"), "logs/www-access.log");

        assertNull(beatsModuleStore.findMatchingModule(APACHE2_ACCESS_LOG_SAMPLE));
    }

    public void testPathMatchesSampleFileNameWithoutPath() {
        assertTrue(BeatsModuleStore.pathMatchesSampleFileNameWithoutPath("/foo/bar/error.log", "error.log"));
        assertFalse(BeatsModuleStore.pathMatchesSampleFileNameWithoutPath("/foo/bar/error.log", "err.log"));
        assertTrue(BeatsModuleStore.pathMatchesSampleFileNameWithoutPath("/foo/bar/error.log", "my-error.log"));
        assertTrue(BeatsModuleStore.pathMatchesSampleFileNameWithoutPath("/foo/bar/{{something}}.log", "my-error.log"));
        assertFalse(BeatsModuleStore.pathMatchesSampleFileNameWithoutPath("/foo/bar/{{something}}.log", "my-log"));
        assertTrue(BeatsModuleStore.pathMatchesSampleFileNameWithoutPath("/foo/bar/{{host}}.{{user}}.log", "mbp.dave.log"));
    }
}
