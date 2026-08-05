/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.DatabaseRecord;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class GeoIpTestUtils {

    private GeoIpTestUtils() {
        // utility class
    }

    public static final Set<String> DEFAULT_DATABASES = Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb");

    @SuppressForbidden(reason = "uses java.io.File")
    private static boolean isDirectory(final Path path) {
        return path.toFile().isDirectory();
    }

    public static void copyDatabase(final String databaseName, final Path destination) {
        try (InputStream is = GeoIpTestUtils.class.getResourceAsStream("/" + databaseName)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + databaseName + "] not found in classpath");
            }

            Files.copy(is, isDirectory(destination) ? destination.resolve(databaseName) : destination, REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void copyDefaultDatabases(final Path directory) {
        for (final String database : DEFAULT_DATABASES) {
            copyDatabase(database, directory);
        }
    }

    public static void copyDefaultDatabases(final Path directory, ConfigDatabases configDatabases) {
        for (final String database : DEFAULT_DATABASES) {
            copyDatabase(database, directory);
            configDatabases.updateDatabase(directory.resolve(database), true);
        }
    }

    /**
     * Returns the path to {@code resource} within {@code sharedDir}, copying it from the test classpath on first
     * access and reusing the same on-disk copy on subsequent calls. The file name on disk is the basename of
     * {@code resource} (any directory prefix such as {@code "ipinfo/"} is stripped).
     *
     * <p>Intended for tests that share a class-scoped read-only mmdb directory across many test methods (and
     * across {@code -Dtests.iters=N} reruns) to avoid re-copying the same database into a fresh per-test
     * temp dir on every invocation. Callers must not mutate the returned file. Synchronized so the helper is
     * also safe to call from tests that exercise concurrency.
     */
    public static synchronized Path resolveSharedDatabase(Path sharedDir, String resource) {
        Path target = sharedDir.resolve(PathUtils.get(resource).getFileName().toString());
        if (Files.notExists(target)) {
            copyDatabase(resource, target);
        }
        return target;
    }

    /**
     * Creates a {@link DatabaseNodeService} configured with default GeoLite2 databases and any additional databases.
     * This factory handles package-private classes ({@link GeoIpCache}, {@link ConfigDatabases}) so that tests in
     * other packages can obtain a fully functional service instance.
     *
     * @param geoIpConfigDir directory where database files will be placed
     * @param tmpDir temporary directory for the service
     * @param projectId project to register in the cluster state
     * @param projectResolver project resolver for the service
     * @param additionalDatabases optional resource paths of extra databases to copy and register (e.g. "ipinfo/foo.mmdb")
     * @return a started {@link DatabaseNodeService}
     */
    public static DatabaseNodeService createTestDatabaseNodeService(
        Path geoIpConfigDir,
        Path tmpDir,
        ProjectId projectId,
        ProjectResolver projectResolver,
        String... additionalDatabases
    ) throws IOException {
        Files.createDirectories(geoIpConfigDir);

        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDefaultDatabases(geoIpConfigDir, configDatabases);

        for (String db : additionalDatabases) {
            String fileName = PathUtils.get(db).getFileName().toString();
            Path target = geoIpConfigDir.resolve(fileName);
            copyDatabase(db, target);
            configDatabases.updateDatabase(target, true);
        }

        return assembleDatabaseNodeService(tmpDir, projectId, projectResolver, cache, configDatabases);
    }

    /**
     * Like {@link #createTestDatabaseNodeService} but assumes {@code geoIpConfigDir} is already populated with
     * every database to register. The directory is treated as read-only; the service still gets its own
     * per-test {@code tmpDir} for any writable state. Use this together with a class-scoped {@code @BeforeClass}
     * that populates {@code geoIpConfigDir} once, so that {@code -Dtests.iters=N} reruns and many test methods
     * within one class do not each copy the same default mmdbs into a fresh temp dir.
     *
     * @param geoIpConfigDir directory already containing every mmdb to register; must not be mutated by tests
     * @param tmpDir per-test mutable temporary directory for the service
     * @param projectId project to register in the cluster state
     * @param projectResolver project resolver for the service
     * @param databasesToRegister basenames of files within {@code geoIpConfigDir} to register with
     *                            {@link ConfigDatabases}
     */
    public static DatabaseNodeService createTestDatabaseNodeServiceFromExistingDir(
        Path geoIpConfigDir,
        Path tmpDir,
        ProjectId projectId,
        ProjectResolver projectResolver,
        String... databasesToRegister
    ) throws IOException {
        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        for (String name : databasesToRegister) {
            configDatabases.updateDatabase(geoIpConfigDir.resolve(name), true);
        }
        return assembleDatabaseNodeService(tmpDir, projectId, projectResolver, cache, configDatabases);
    }

    private static DatabaseNodeService assembleDatabaseNodeService(
        Path tmpDir,
        ProjectId projectId,
        ProjectResolver projectResolver,
        GeoIpCache cache,
        ConfigDatabases configDatabases
    ) throws IOException {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        when(clusterService.state()).thenReturn(state);

        DatabaseNodeService service = new DatabaseNodeService(
            tmpDir,
            mock(Client.class),
            cache,
            configDatabases,
            Runnable::run,
            clusterService,
            projectResolver
        );
        service.initialize("nodeId", mock(ResourceWatcherService.class));
        return service;
    }

    /**
     * A static city-specific responseProvider for use with {@link IpDatabase#getResponse(String, CheckedBiFunction)} in
     * tests.
     * <p>
     * Like this: {@code CityResponse city = loader.getResponse("some.ip.address", GeoIpTestUtils::getCity);}
     */
    public static CityResponse getCity(Reader reader, String ip) throws IOException {
        DatabaseRecord<CityResponse> record = reader.getRecord(InetAddresses.forString(ip), CityResponse.class);
        CityResponse data = record.getData();
        return data == null ? null : new CityResponse(data, ip, record.getNetwork(), List.of("en"));
    }

    /**
     * A static country-specific responseProvider for use with {@link IpDatabase#getResponse(String, CheckedBiFunction)} in
     * tests.
     * <p>
     * Like this: {@code CountryResponse country = loader.getResponse("some.ip.address", GeoIpTestUtils::getCountry);}
     */
    public static CountryResponse getCountry(Reader reader, String ip) throws IOException {
        DatabaseRecord<CountryResponse> record = reader.getRecord(InetAddresses.forString(ip), CountryResponse.class);
        CountryResponse data = record.getData();
        return data == null ? null : new CountryResponse(data, ip, record.getNetwork(), List.of("en"));
    }
}
