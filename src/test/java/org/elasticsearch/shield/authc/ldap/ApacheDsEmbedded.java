/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.carrotsearch.randomizedtesting.SysGlobals;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.model.schema.registries.SchemaLoader;
import org.apache.directory.api.ldap.schemaextractor.SchemaLdifExtractor;
import org.apache.directory.api.ldap.schemaextractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schemaloader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schemamanager.impl.DefaultSchemaManager;
import org.apache.directory.api.util.exception.Exceptions;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.CacheService;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.DnFactory;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.partition.Partition;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmIndex;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.i18n.I18n;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.store.LdifFileLoader;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Helper Class to start up an Apache DS LDAP server for testing.  Here is a typical use example in tests:
 * <pre>
 * static ApacheDsEmbedded ldap = new ApacheDsEmbedded("o=sevenSeas", "seven-seas.ldif");
 *
 * @BeforeClass public static void startServer() throws Exception {
 * ldap.startServer();
 * }
 * @AfterClass public static void stopServer() throws Exception {
 * ldap.stopAndCleanup();
 * }
 * </pre>
 */
public class ApacheDsEmbedded {
    /**
     * The child JVM ordinal of this JVM. Placed by the testing framework.  Default is <tt>0</tt>  Sequential number starting with 0
     */
    public static final int CHILD_JVM_ID = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    private final File workDir;
    private final String baseDN;
    private final String ldifFileName;
    private final int port;
    /**
     * The directory service
     */
    private DirectoryService service;

    /**
     * The LDAP server
     */
    private LdapServer server;


    /**
     * Creates a new instance of EmbeddedADS. It initializes the directory service.
     *
     * @throws Exception If something went wrong
     */
    public ApacheDsEmbedded(String baseDN, String ldifFileName, String testName) {
        this.workDir = new File(System.getProperty("java.io.tmpdir") + "/server-work/" + testName);
        this.baseDN = baseDN;
        this.ldifFileName = ldifFileName;
        this.port = 10389 + CHILD_JVM_ID;
    }

    public int getPort() {
        return port;
    }


    public String getUrl() {
        return "ldap://localhost:" + port;
    }

    /**
     * starts the LdapServer
     *
     * @throws Exception
     */
    public void startServer() throws Exception {
        initDirectoryService(workDir, baseDN, ldifFileName);
        loadSchema(ldifFileName);

        server = new LdapServer();
        server.setTransports(new TcpTransport(port));
        server.setDirectoryService(service);

        server.start();
    }

    /**
     * This will cleanup the junk left on the file system and shutdown the server
     *
     * @throws Exception
     */
    public void stopAndCleanup() throws Exception {
        if (server != null) server.stop();
        if (service != null) service.shutdown();
        workDir.delete();
    }


    /**
     * Add a new partition to the server
     *
     * @param partitionId The partition Id
     * @param partitionDn The partition DN
     * @param dnFactory   the DN factory
     * @return The newly added partition
     * @throws Exception If the partition can't be added
     */
    private Partition addPartition(String partitionId, String partitionDn, DnFactory dnFactory) throws Exception {
        // Create a new partition with the given partition id
        JdbmPartition partition = new JdbmPartition(service.getSchemaManager(), dnFactory);
        partition.setId(partitionId);
        partition.setPartitionPath(new File(service.getInstanceLayout().getPartitionsDirectory(), partitionId).toURI());
        partition.setSuffixDn(new Dn(partitionDn));
        service.addPartition(partition);

        return partition;
    }


    /**
     * Add a new set of index on the given attributes
     *
     * @param partition The partition on which we want to add index
     * @param attrs     The list of attributes to index
     */
    private void addIndex(Partition partition, String... attrs) {
        // Index some attributes on the apache partition
        Set indexedAttributes = new HashSet();

        for (String attribute : attrs) {
            indexedAttributes.add(new JdbmIndex(attribute, false));
        }

        ((JdbmPartition) partition).setIndexedAttributes(indexedAttributes);
    }


    /**
     * initialize the schema manager and add the schema partition to diectory service
     *
     * @throws Exception if the schema LDIF files are not found on the classpath
     */
    private void initSchemaPartition() throws Exception {
        InstanceLayout instanceLayout = service.getInstanceLayout();

        File schemaPartitionDirectory = new File(instanceLayout.getPartitionsDirectory(), "schema");

        // Extract the schema on disk (a brand new one) and load the registries
        if (schemaPartitionDirectory.exists()) {
            System.out.println("schema partition already exists, skipping schema extraction");
        } else {
            SchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(instanceLayout.getPartitionsDirectory());
            extractor.extractOrCopy();
        }

        SchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
        SchemaManager schemaManager = new DefaultSchemaManager(loader);

        // We have to load the schema now, otherwise we won't be able
        // to initialize the Partitions, as we won't be able to parse
        // and normalize their suffix Dn
        schemaManager.loadAllEnabled();

        List<Throwable> errors = schemaManager.getErrors();

        if (errors.size() != 0) {
            throw new Exception(I18n.err(I18n.ERR_317, Exceptions.printErrors(errors)));
        }

        service.setSchemaManager(schemaManager);

        // Init the LdifPartition with schema
        LdifPartition schemaLdifPartition = new LdifPartition(schemaManager, service.getDnFactory());
        schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI());

        // The schema partition
        SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        schemaPartition.setWrappedPartition(schemaLdifPartition);
        service.setSchemaPartition(schemaPartition);
    }

    /**
     * Initialize the server. It creates the partition, adds the index, and
     * injects the context entries for the created partitions.
     *
     * @param workDir      the directory to be used for storing the data
     * @param baseDn
     * @param ldifFileName @throws Exception if there were some problems while initializing the system
     */
    private void initDirectoryService(File workDir, String baseDn, String ldifFileName) throws Exception {
        // Initialize the LDAP service
        service = new DefaultDirectoryService();
        service.setInstanceLayout(new InstanceLayout(workDir));

        CacheService cacheService = new CacheService();
        cacheService.initialize(service.getInstanceLayout());

        service.setCacheService(cacheService);

        // first load the schema
        initSchemaPartition();

        // then the system partition
        // this is a MANDATORY partition
        // DO NOT add this via addPartition() method, trunk code complains about duplicate partition
        // while initializing
        JdbmPartition systemPartition = new JdbmPartition(service.getSchemaManager(), service.getDnFactory());
        systemPartition.setId("system");
        systemPartition.setPartitionPath(new File(service.getInstanceLayout().getPartitionsDirectory(), systemPartition.getId()).toURI());
        systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN));
        systemPartition.setSchemaManager(service.getSchemaManager());

        // mandatory to call this method to set the system partition
        // Note: this system partition might be removed from trunk
        service.setSystemPartition(systemPartition);

        // Disable the ChangeLog system
        service.getChangeLog().setEnabled(false);
        service.setDenormalizeOpAttrsEnabled(true);

        Partition apachePartition = addPartition("ldapTest", baseDn, service.getDnFactory());


        // Index some attributes on the apache partition
        addIndex(apachePartition, "objectClass", "ou", "uid");

        // And start the service
        service.startup();


        // Inject the context entry for dc=Apache,dc=Org partition
        if (!service.getAdminSession().exists(apachePartition.getSuffixDn())) {
            Dn dnApache = new Dn(baseDn);
            Entry entryApache = service.newEntry(dnApache);
            entryApache.add("objectClass", "top", "domain", "extensibleObject");
            entryApache.add("dc", "Apache");
            service.getAdminSession().add(entryApache);
        }

        // We are all done !
    }

    private void loadSchema(String ldifFileName) throws URISyntaxException {
        // Load the directory as a resource
        URL dir_url = this.getClass().getResource(ldifFileName);
        if (dir_url == null) throw new NullPointerException("the LDIF file doesn't exist: " + ldifFileName);
        File ldifFile = new File(dir_url.toURI());
        LdifFileLoader ldifLoader = new LdifFileLoader(service.getAdminSession(), ldifFile, Collections.EMPTY_LIST);
        ldifLoader.execute();
    }

    /**
     * Main class.
     *
     * @param args Not used.
     */
    public static void main(String[] args) {
        try {
            String baseDir = "o=sevenSeas";
            String ldifImport = "seven-seas.ldif";
            // Create the server
            ApacheDsEmbedded ads = new ApacheDsEmbedded(baseDir, ldifImport, "test");
            ads.startServer();
            // Read an entry
            Entry result = ads.service.getAdminSession().lookup(new Dn(baseDir));

            // And print it if available
            System.out.println("Found entry : " + result);

            // optionally we can start a server too
            ads.stopAndCleanup();
        } catch (Exception e) {
            // Ok, we have something wrong going on ...
            e.printStackTrace();
        }
    }
}
