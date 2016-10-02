package org.elasticsearch.ingest.attachment;

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

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;

import java.io.ByteArrayInputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.lang.reflect.ReflectPermission;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.security.SecurityPermission;
import java.util.PropertyPermission;

/**
 * Runs tika with limited parsers and limited permissions.
 * <p>
 * Do NOT make public
 */
final class TikaImpl {

    /** subset of parsers for types we support */
    private static final Parser PARSERS[] = new Parser[] {
        // documents
        new org.apache.tika.parser.html.HtmlParser(),
        new org.apache.tika.parser.rtf.RTFParser(),
        new org.apache.tika.parser.pdf.PDFParser(),
        new org.apache.tika.parser.txt.TXTParser(),
        new org.apache.tika.parser.microsoft.OfficeParser(),
        new org.apache.tika.parser.microsoft.OldExcelParser(),
        new org.apache.tika.parser.microsoft.ooxml.OOXMLParser(),
        new org.apache.tika.parser.odf.OpenDocumentParser(),
        new org.apache.tika.parser.iwork.IWorkPackageParser(),
        new org.apache.tika.parser.xml.DcXMLParser(),
        new org.apache.tika.parser.epub.EpubParser(),
    };

    /** autodetector based on this subset */
    private static final AutoDetectParser PARSER_INSTANCE = new AutoDetectParser(PARSERS);

    /** singleton tika instance */
    private static final Tika TIKA_INSTANCE = new Tika(PARSER_INSTANCE.getDetector(), PARSER_INSTANCE);

    /**
     * parses with tika, throwing any exception hit while parsing the document
     */
    // only package private for testing!
    static String parse(final byte content[], final Metadata metadata, final int limit) throws TikaException, IOException {
        // check that its not unprivileged code like a script
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws TikaException, IOException {
                    return TIKA_INSTANCE.parseToString(new ByteArrayInputStream(content), metadata, limit);
                }
            }, RESTRICTED_CONTEXT);
        } catch (PrivilegedActionException e) {
            // checked exception from tika: unbox it
            Throwable cause = e.getCause();
            if (cause instanceof TikaException) {
                throw (TikaException) cause;
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new AssertionError(cause);
            }
        }
    }

    // apply additional containment for parsers, this is intersected with the current permissions
    // its hairy, but worth it so we don't have some XML flaw reading random crap from the FS
    private static final AccessControlContext RESTRICTED_CONTEXT = new AccessControlContext(
        new ProtectionDomain[] {
            new ProtectionDomain(null, getRestrictedPermissions())
        }
    );

    // compute some minimal permissions for parsers. they only get r/w access to the java temp directory,
    // the ability to load some resources from JARs, and read sysprops
    static PermissionCollection getRestrictedPermissions() {
        Permissions perms = new Permissions();
        // property/env access needed for parsing
        perms.add(new PropertyPermission("*", "read"));
        perms.add(new RuntimePermission("getenv.TIKA_CONFIG"));

        // add permissions for resource access:
        // classpath
        addReadPermissions(perms, JarHell.parseClassPath());
        // plugin jars
        if (TikaImpl.class.getClassLoader() instanceof URLClassLoader) {
            addReadPermissions(perms, ((URLClassLoader)TikaImpl.class.getClassLoader()).getURLs());
        }
        // jvm's java.io.tmpdir (needs read/write)
        perms.add(new FilePermission(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "-",
                                     "read,readlink,write,delete"));
        // current hacks needed for POI/PDFbox issues:
        perms.add(new SecurityPermission("putProviderProperty.BC"));
        perms.add(new SecurityPermission("insertProvider"));
        perms.add(new ReflectPermission("suppressAccessChecks"));
        // xmlbeans, use by POI, needs to get the context classloader
        perms.add(new RuntimePermission("getClassLoader"));
        perms.setReadOnly();
        return perms;
    }

    // add resources to (what is typically) a jar, but might not be (e.g. in tests/IDE)
    @SuppressForbidden(reason = "adds access to jar resources")
    static void addReadPermissions(Permissions perms, URL resources[]) {
        try {
            for (URL url : resources) {
                Path path = PathUtils.get(url.toURI());
                // resource itself
                perms.add(new FilePermission(path.toString(), "read,readlink"));
                // classes underneath
                perms.add(new FilePermission(path.toString() + System.getProperty("file.separator") + "-", "read,readlink"));
            }
        } catch (URISyntaxException bogus) {
            throw new RuntimeException(bogus);
        }
    }
}
