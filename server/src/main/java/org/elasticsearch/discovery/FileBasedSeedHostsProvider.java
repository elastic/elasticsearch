/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * An implementation of {@link SeedHostsProvider} that reads hosts/ports
 * from {@link #UNICAST_HOSTS_FILE}.
 *
 * Each host/port that is part of the discovery process must be listed on
 * a separate line. If the port is left off an entry, we default to the
 * first port in the {@code transport.port} range.
 * An example unicast hosts file could read:
 *
 * 67.81.244.10
 * 67.81.244.11:9305
 * 67.81.244.15:9400
 */
public class FileBasedSeedHostsProvider implements SeedHostsProvider {

    private static final Logger logger = LogManager.getLogger(FileBasedSeedHostsProvider.class);

    public static final String UNICAST_HOSTS_FILE = "unicast_hosts.txt";

    private final Path unicastHostsFilePath;

    public FileBasedSeedHostsProvider(Path configFile) {
        this.unicastHostsFilePath = configFile.resolve(UNICAST_HOSTS_FILE);
    }

    private List<String> getHostsList() {
        if (Files.exists(unicastHostsFilePath)) {
            try (Stream<String> lines = Files.lines(unicastHostsFilePath)) {
                return lines.filter(line -> line.startsWith("#") == false) // lines starting with `#` are comments
                    .toList();
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("failed to read file [{}]", unicastHostsFilePath), e);
                return Collections.emptyList();
            }
        }

        logger.warn("expected, but did not find, a dynamic hosts list at [{}]", unicastHostsFilePath);

        return Collections.emptyList();
    }

    @Override
    public List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver) {
        final List<TransportAddress> transportAddresses = hostsResolver.resolveHosts(getHostsList());
        logger.debug("seed addresses: {}", transportAddresses);
        return transportAddresses;
    }
}
