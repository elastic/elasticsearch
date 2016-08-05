/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import com.google.common.base.Charsets;
import javax.net.ssl.HttpsURLConnection;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.SettingCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.file.FileUserPasswdStore;
import org.elasticsearch.xpack.security.authc.file.FileUserRolesStore;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.security.Security.setting;

/**
 * This is the command-line tool used for migrating users and roles from the file-based realm into the new native realm using the API for
 * import. It reads from the files and tries its best to add the users, showing an error if it was incapable of importing them. Any existing
 * users or roles are skipped.
 */
public class ESNativeRealmMigrateTool extends MultiCommand {

    public static void main(String[] args) throws Exception {
        exit(new ESNativeRealmMigrateTool().main(args, Terminal.DEFAULT));
    }

    public ESNativeRealmMigrateTool() {
        super("Imports file-based users and roles to the native security realm");
        subcommands.put("native", new MigrateUserOrRoles());
    }

    /** Command to migrate users and roles to the native realm */
    public static class MigrateUserOrRoles extends SettingCommand {

        private final OptionSpec<String> username;
        private final OptionSpec<String> password;
        private final OptionSpec<String> url;
        private final OptionSpec<String> usersToMigrateCsv;
        private final OptionSpec<String> rolesToMigrateCsv;
        private final OptionSpec<String> esConfigDir;

        public MigrateUserOrRoles() {
            super("Migrates users or roles from file to native realm");
            this.username = parser.acceptsAll(Arrays.asList("u", "username"),
                    "User used to authenticate with Elasticsearch")
                    .withRequiredArg();
            this.password = parser.acceptsAll(Arrays.asList("p", "password"),
                    "Password used to authenticate with Elasticsearch")
                    .withRequiredArg();
            this.url = parser.acceptsAll(Arrays.asList("U", "url"),
                    "URL of Elasticsearch host")
                    .withRequiredArg();
            this.usersToMigrateCsv = parser.acceptsAll(Arrays.asList("n", "users"),
                    "Users to migrate from file to native realm")
                    .withRequiredArg();
            this.rolesToMigrateCsv = parser.acceptsAll(Arrays.asList("r", "roles"),
                    "Roles to migrate from file to native realm")
                    .withRequiredArg();
            this.esConfigDir = parser.acceptsAll(Arrays.asList("c", "config"),
                    "Configuration directory to use instead of default")
                    .withRequiredArg();
        }

        // Visible for testing
        public OptionParser getParser() {
            return this.parser;
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("This tool migrates file based users[1] and roles[2] to the native realm in");
            terminal.println("elasticsearch, saving the administrator from needing to manually transition");
            terminal.println("them from the file.");
        }

        // Visible for testing
        @Override
        public void execute(Terminal terminal, OptionSet options, Map<String, String> settings) throws Exception {
            terminal.println("starting migration of users and roles...");
            Settings.Builder sb = Settings.builder();
            sb.put(settings);
            if (this.esConfigDir != null) {
                sb.put("path.conf", this.esConfigDir.value(options));
            }
            Settings shieldSettings = sb.build();
            Environment shieldEnv = new Environment(shieldSettings);
            importUsers(terminal, shieldSettings, shieldEnv, options);
            importRoles(terminal, shieldSettings, shieldEnv, options);
            terminal.println("users and roles imported.");
        }

        private String postURL(Settings settings, Environment env, String method, String urlString,
                               OptionSet options, @Nullable String bodyString) throws Exception {
            URI uri = new URI(urlString);
            URL url = uri.toURL();
            HttpURLConnection conn;
            // If using SSL, need a custom service because it's likely a self-signed certificate
            if ("https".equalsIgnoreCase(uri.getScheme())) {
                Settings sslSettings = settings.getByPrefix(setting("http.ssl."));
                SSLConfiguration.Global globalConfig = new SSLConfiguration.Global(settings);
                final ClientSSLService sslService = new ClientSSLService(sslSettings, env, globalConfig);
                final HttpsURLConnection httpsConn = (HttpsURLConnection) url.openConnection();
                AccessController.doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() {
                        // Requires permission java.lang.RuntimePermission "setFactory";
                        httpsConn.setSSLSocketFactory(sslService.sslSocketFactory(sslSettings));
                        return null;
                    }
                });
                conn = httpsConn;
            } else {
                conn = (HttpURLConnection) url.openConnection();
            }
            conn.setRequestMethod(method);
            conn.setReadTimeout(30 * 1000); // 30 second timeout
            // Add basic-auth header
            conn.setRequestProperty("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(username.value(options),
                            new SecuredString(password.value(options).toCharArray())));
            conn.setDoOutput(true); // we'll be sending a body
            conn.connect();
            if (bodyString != null) {
                try (OutputStream out = conn.getOutputStream()) {
                    out.write(bodyString.getBytes(Charsets.UTF_8));
                } catch (Exception e) {
                    try {
                        conn.disconnect();
                    } catch (Exception e2) {
                        // Ignore exceptions if we weren't able to close the connection after an error
                    }
                    throw e;
                }
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), Charsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return sb.toString();
            } catch (IOException e) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), Charsets.UTF_8))) {
                    StringBuilder sb = new StringBuilder();
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    throw new IOException(sb.toString(), e);
                }
            } finally {
                conn.disconnect();
            }
        }

        public Set<String> getUsersThatExist(Terminal terminal, Settings settings, Environment env, OptionSet options) throws Exception {
            Set<String> existingUsers = new HashSet<>();
            String allUsersJson = postURL(settings, env, "GET", this.url.value(options) + "/_xpack/security/user/", options, null);
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(allUsersJson)) {
                XContentParser.Token token = parser.nextToken();
                String userName;
                if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
                        userName = parser.currentName();
                        existingUsers.add(userName);
                        parser.nextToken();
                        parser.skipChildren();
                    }
                } else {
                    throw new ElasticsearchException("failed to retrieve users, expecting an object but got: " + token);
                }
            }
            terminal.println("found existing users: " + existingUsers);
            return existingUsers;
        }

        public static String createUserJson(String[] roles, char[] password) throws IOException {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.field("password_hash", new String(password));
                builder.startArray("roles");
                for (String role : roles) {
                    builder.value(role);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder.string();
        }

        public void importUsers(Terminal terminal, Settings settings, Environment env, OptionSet options) {
            String usersCsv = usersToMigrateCsv.value(options);
            String[] usersToMigrate = (usersCsv != null) ? usersCsv.split(",") : Strings.EMPTY_ARRAY;
            Settings fileRealmSettings = Realms.fileRealmSettings(settings);
            Path usersFile = FileUserPasswdStore.resolveFile(fileRealmSettings, env);
            Path usersRolesFile = FileUserRolesStore.resolveFile(fileRealmSettings, env);
            terminal.println("importing users from [" + usersFile + "]...");
            Map<String, char[]> userToHashedPW = FileUserPasswdStore.parseFile(usersFile, null);
            Map<String, String[]> userToRoles = FileUserRolesStore.parseFile(usersRolesFile, null);
            Set<String> existingUsers;
            try {
                existingUsers = getUsersThatExist(terminal, settings, env, options);
            } catch (Exception e) {
                throw new ElasticsearchException("failed to get users that already exist, skipping user import", e);
            }
            if (usersToMigrate.length == 0) {
                usersToMigrate = userToHashedPW.keySet().toArray(new String[userToHashedPW.size()]);
            }
            for (String user : usersToMigrate) {
                if (userToHashedPW.containsKey(user) == false) {
                    terminal.println("no user [" + user + "] found, skipping");
                    continue;
                } else if (existingUsers.contains(user)) {
                    terminal.println("user [" + user + "] already exists, skipping");
                    continue;
                }
                terminal.println("migrating user [" + user + "]");
                String reqBody = "n/a";
                try {
                    reqBody = createUserJson(userToRoles.get(user), userToHashedPW.get(user));
                    String resp = postURL(settings, env, "POST",
                            this.url.value(options) + "/_xpack/security/user/" + user, options, reqBody);
                    terminal.println(resp);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to migrate user [" + user + "] with body: " + reqBody, e);
                }
            }
        }

        public Set<String> getRolesThatExist(Terminal terminal, Settings settings, Environment env, OptionSet options) throws Exception {
            Set<String> existingRoles = new HashSet<>();
            String allRolesJson = postURL(settings, env, "GET", this.url.value(options) + "/_xpack/security/role/", options, null);
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(allRolesJson)) {
                XContentParser.Token token = parser.nextToken();
                String roleName;
                if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
                        roleName = parser.currentName();
                        existingRoles.add(roleName);
                        parser.nextToken();
                        parser.skipChildren();
                    }
                } else {
                    throw new ElasticsearchException("failed to retrieve roles, expecting an object but got: " + token);
                }
            }
            terminal.println("found existing roles: " + existingRoles);
            return existingRoles;
        }

        public static String createRoleJson(RoleDescriptor rd) throws IOException {
            XContentBuilder builder = jsonBuilder();
            rd.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.string();
        }

        public void importRoles(Terminal terminal, Settings settings, Environment env, OptionSet options) {
            String rolesCsv = rolesToMigrateCsv.value(options);
            String[] rolesToMigrate = (rolesCsv != null) ? rolesCsv.split(",") : Strings.EMPTY_ARRAY;
            Settings fileRealmSettings = Realms.fileRealmSettings(settings);
            Path rolesFile = FileRolesStore.resolveFile(fileRealmSettings, env).toAbsolutePath();
            terminal.println("importing roles from [" + rolesFile + "]...");
            Map<String, RoleDescriptor> roles = FileRolesStore.parseRoleDescriptors(rolesFile, null, true, Settings.EMPTY);
            Set<String> existingRoles;
            try {
                existingRoles = getRolesThatExist(terminal, settings, env, options);
            } catch (Exception e) {
                throw new ElasticsearchException("failed to get roles that already exist, skipping role import", e);
            }
            if (rolesToMigrate.length == 0) {
                rolesToMigrate = roles.keySet().toArray(new String[roles.size()]);
            }
            for (String roleName : rolesToMigrate) {
                if (roles.containsKey(roleName) == false) {
                    terminal.println("no role [" + roleName + "] found, skipping");
                    continue;
                } else if (existingRoles.contains(roleName)) {
                    terminal.println("role [" + roleName + "] already exists, skipping");
                    continue;
                }
                terminal.println("migrating role [" + roleName + "]");
                String reqBody = "n/a";
                try {
                    reqBody = createRoleJson(roles.get(roleName));;
                    String resp = postURL(settings, env, "POST",
                            this.url.value(options) + "/_xpack/security/role/" + roleName, options, reqBody);
                    terminal.println(resp);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to migrate role [" + roleName + "] with body: " + reqBody, e);
                }
            }
        }
    }
}
