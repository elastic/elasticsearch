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

package org.elasticsearch.discovery.file;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.env.Environment;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of {@link UnicastHostsProvider} that reads hosts/ports
 * from {@link #UNICAST_HOSTS_FILE}.
 *
 * Each unicast host/port that is part of the discovery process must be listed on
 * a separate line.  If the port is left off an entry, a default port of 9300 is
 * assumed.  An example unicast hosts file could read:
 *
 * 67.81.244.10
 * 67.81.244.11:9305
 * 67.81.244.15:9400
 */
class FileBasedUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    static final String UNICAST_HOSTS_FILE = "unicast_hosts.txt";

    private final Path unicastHostsFilePath;

    FileBasedUnicastHostsProvider(Environment environment) {
        super(environment.settings());
        this.unicastHostsFilePath = environment.configFile().resolve("discovery-file").resolve(UNICAST_HOSTS_FILE);
    }

    @Override
    public List<TransportAddress> buildDynamicHosts(HostsResolver hostsResolver) {
        List<String> hostsList;
        try (Stream<String> lines = Files.lines(unicastHostsFilePath)) {
            hostsList = lines.filter(line -> line.startsWith("#") == false) // lines starting with `#` are comments
                             .collect(Collectors.toList());
        } catch (FileNotFoundException | NoSuchFileException e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[discovery-file] Failed to find unicast hosts file [{}]",
                                                                        unicastHostsFilePath), e);
            hostsList = Collections.emptyList();
        } catch (IOException e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[discovery-file] Error reading unicast hosts file [{}]",
                                                                        unicastHostsFilePath), e);
            hostsList = Collections.emptyList();
        }

        final List<TransportAddress> dynamicHosts = hostsResolver.resolveHosts(hostsList, 1);
        logger.debug("[discovery-file] Using dynamic discovery nodes {}", dynamicHosts);
        return dynamicHosts;
    }

}
