/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper.UserData;
import org.ietf.jgss.GSSException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import javax.security.auth.login.LoginException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class KerberosRealmTests extends KerberosRealmTestCase {

    public void testSupports() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(randomByteArrayOfLength(5));
        assertThat(kerberosRealm.supports(kerberosAuthenticationToken), is(true));
        final UsernamePasswordToken usernamePasswordToken =
                new UsernamePasswordToken(randomAlphaOfLength(5), new SecureString(new char[] { 'a', 'b', 'c' }));
        assertThat(kerberosRealm.supports(usernamePasswordToken), is(false));
    }

    public void testAuthenticateWithValidTicketSucessAuthnWithUserDetails() throws LoginException, GSSException {
        final String username = randomPrincipalName();
        final KerberosRealm kerberosRealm = createKerberosRealm(username);
        final String expectedUsername = maybeRemoveRealmName(username);
        final User expectedUser = new User(expectedUsername, roles.toArray(new String[roles.size()]), null, null, null, true);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(username, "out-token"), null);
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        assertSuccessAuthenticationResult(expectedUser, "out-token", future.actionGet());

        verify(mockKerberosTicketValidator, times(1)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                any(ActionListener.class));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);
        verify(mockNativeRoleMappingStore).resolveRoles(any(UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }

    public void testFailedAuthorization() throws LoginException, GSSException {
        final String username = randomPrincipalName();
        final KerberosRealm kerberosRealm = createKerberosRealm(username);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>("does-not-exist@REALM", "out-token"), null);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(new KerberosAuthenticationToken(decodedTicket), future);

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), equalTo("Expected UPN '" + Arrays.asList(maybeRemoveRealmName(username)) + "' but was '"
                + maybeRemoveRealmName("does-not-exist@REALM") + "'"));
    }

    public void testLookupUser() {
        final String username = randomPrincipalName();
        final KerberosRealm kerberosRealm = createKerberosRealm(username);
        final PlainActionFuture<User> future = new PlainActionFuture<>();
        kerberosRealm.lookupUser(username, future);
        assertThat(future.actionGet(), is(nullValue()));
    }

    public void testKerberosRealmThrowsErrorWhenKeytabPathIsConfiguredAsDirectory() throws IOException {
        final String dirName = randomAlphaOfLength(5);
        Files.createDirectory(dir.resolve(dirName));
        final String keytabPath = dir.resolve(dirName).toString();
        final String expectedErrorMessage = "configured service key tab file [" + keytabPath + "] is a directory";

        assertKerberosRealmConstructorFails(keytabPath, expectedErrorMessage);
    }

    public void testKerberosRealmThrowsErrorWhenKeytabFileDoesNotExist() throws IOException {
        final String keytabPath = dir.resolve(randomAlphaOfLength(5) + ".keytab").toString();
        final String expectedErrorMessage = "configured service key tab file [" + keytabPath + "] does not exist";

        assertKerberosRealmConstructorFails(keytabPath, expectedErrorMessage);
    }

    public void testKerberosRealmThrowsErrorWhenKeytabFileHasNoReadPermissions() throws IOException {
        assumeFalse("Not running this test on Windows, as it requires additional access permissions for test framework.",
                Constants.WINDOWS);
        final Set<String> supportedAttributes = dir.getFileSystem().supportedFileAttributeViews();
        final String keytabFileName = randomAlphaOfLength(5) + ".keytab";
        final Path keytabPath;
        if (supportedAttributes.contains("posix")) {
            final Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("---------");
            final FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(filePerms);
            try (SeekableByteChannel byteChannel = Files.newByteChannel(dir.resolve(keytabFileName),
                    EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), fileAttributes)) {
                byteChannel.write(ByteBuffer.wrap(randomByteArrayOfLength(10)));
            }
            keytabPath = dir.resolve(keytabFileName);
        } else {
            throw new UnsupportedOperationException(
                    String.format(Locale.ROOT, "Don't know how to make file [%s] non-readable on a file system with attributes [%s]",
                            dir.resolve(keytabFileName), supportedAttributes));
        }
        final String expectedErrorMessage = "configured service key tab file [" + keytabPath + "] must have read permission";

        assertKerberosRealmConstructorFails(keytabPath.toString(), expectedErrorMessage);
    }

    private void assertKerberosRealmConstructorFails(final String keytabPath, final String expectedErrorMessage) {
        settings = KerberosTestCase.buildKerberosRealmSettings(keytabPath, 100, "10m", true, randomBoolean());
        config = new RealmConfig("test-kerb-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings));
        mockNativeRoleMappingStore = roleMappingStore(Arrays.asList("user"));
        mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> new KerberosRealm(config, mockNativeRoleMappingStore, mockKerberosTicketValidator, threadPool, null));
        assertThat(iae.getMessage(), is(equalTo(expectedErrorMessage)));
    }
}
