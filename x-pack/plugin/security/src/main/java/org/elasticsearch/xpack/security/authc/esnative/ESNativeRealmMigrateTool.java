/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.file.FileUserPasswdStore;
import org.elasticsearch.xpack.security.authc.file.FileUserRolesStore;

import javax.net.ssl.HttpsURLConnection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * This is the command-line tool used for migrating users and roles from the file-based realm into the new native realm using the API for
 * import. It reads from the files and tries its best to add the users, showing an error if it was incapable of importing them. Any existing
 * users or roles are skipped.
 */
public class ESNativeRealmMigrateTool extends LoggingAwareMultiCommand {

    public static void main(String[] args) throws Exception {
        exit(new ESNativeRealmMigrateTool().main(args, Terminal.DEFAULT));
    }

    public ESNativeRealmMigrateTool() {
        super("Imports file-based users and roles to the native security realm");
        subcommands.put("native", newMigrateUserOrRoles());
    }

    protected MigrateUserOrRoles newMigrateUserOrRoles() {
        return new MigrateUserOrRoles();
    }

    /**
     * Command to migrate users and roles to the native realm
     */
    public static class MigrateUserOrRoles extends EnvironmentAwareCommand {

        private final OptionSpec<String> username;
        private final OptionSpec<String> password;
        private final OptionSpec<String> url;
        private final OptionSpec<String> usersToMigrateCsv;
        private final OptionSpec<String> rolesToMigrateCsv;

        public MigrateUserOrRoles() {
            super("Migrates users or roles from file to native realm");
            this.username = parser.acceptsAll(Arrays.asList("u", "username"),
                    "User used to authenticate with Elasticsearch")
                    .withRequiredArg().required();
            this.password = parser.acceptsAll(Arrays.asList("p", "password"),
                    "Password used to authenticate with Elasticsearch")
                    .withRequiredArg().required();
            this.url = parser.acceptsAll(Arrays.asList("U", "url"),
                    "URL of Elasticsearch host")
                    .withRequiredArg();
            this.usersToMigrateCsv = parser.acceptsAll(Arrays.asList("n", "users"),
                    "Users to migrate from file to native realm")
                    .withRequiredArg();
            this.rolesToMigrateCsv = parser.acceptsAll(Arrays.asList("r", "roles"),
                    "Roles to migrate from file to native realm")
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
        public void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            terminal.println("starting migration of users and roles...");
            importUsers(terminal, env, options);
            importRoles(terminal, env, options);
            terminal.println("users and roles imported.");
        }

        @SuppressForbidden(reason = "We call connect in doPrivileged and provide SocketPermission")
        private String postURL(Settings settings, Environment env, String method, String urlString,
                               OptionSet options, @Nullable String bodyString) throws Exception {
            URI uri = new URI(urlString);
            URL url = uri.toURL();
            HttpURLConnection conn;
            // If using SSL, need a custom service because it's likely a self-signed certificate
            if ("https".equalsIgnoreCase(uri.getScheme())) {
                final SSLService sslService = new SSLService(settings, env);
                final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(setting("http.ssl"));
                final HttpsURLConnection httpsConn = (HttpsURLConnection) url.openConnection();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    // Requires permission java.lang.RuntimePermission "setFactory";
                    httpsConn.setSSLSocketFactory(sslService.sslSocketFactory(sslConfiguration));
                    return null;
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
                            new SecureString(password.value(options).toCharArray())));
            conn.setRequestProperty("Content-Type", XContentType.JSON.mediaType());
            conn.setDoOutput(true); // we'll be sending a body
            SocketAccess.doPrivileged(conn::connect);
            if (bodyString != null) {
                try (OutputStream out = conn.getOutputStream()) {
                    out.write(bodyString.getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    try {
                        conn.disconnect();
                    } catch (Exception e2) {
                        // Ignore exceptions if we weren't able to close the connection after an error
                    }
                    throw e;
                }
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return sb.toString();
            } catch (IOException e) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
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

        Set<String> getUsersThatExist(Terminal terminal, Settings settings, Environment env, OptionSet options) throws Exception {
            Set<String> existingUsers = new HashSet<>();
            String allUsersJson = postURL(settings, env, "GET", this.url.value(options) + "/_security/user/", options, null);
            // EMPTY is safe here because we never use namedObject
            try (XContentParser parser = JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, allUsersJson)) {
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

        static String createUserJson(String[] roles, char[] password) throws IOException {
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
            return Strings.toString(builder);
        }

        void importUsers(Terminal terminal, Environment env, OptionSet options) throws FileNotFoundException {
            String usersCsv = usersToMigrateCsv.value(options);
            String[] usersToMigrate = (usersCsv != null) ? usersCsv.split(",") : Strings.EMPTY_ARRAY;
            Path usersFile = FileUserPasswdStore.resolveFile(env);
            Path usersRolesFile = FileUserRolesStore.resolveFile(env);
            if (Files.exists(usersFile) == false) {
                throw new FileNotFoundException("users file [" + usersFile + "] does not exist");
            } else if (Files.exists(usersRolesFile) == false) {
                throw new FileNotFoundException("users_roles file [" + usersRolesFile + "] does not exist");
            }

            terminal.println("importing users from [" + usersFile + "]...");
            final Logger logger = getTerminalLogger(terminal);
            Map<String, char[]> userToHashedPW = FileUserPasswdStore.parseFile(usersFile, logger, env.settings());
            Map<String, String[]> userToRoles = FileUserRolesStore.parseFile(usersRolesFile, logger);
            Set<String> existingUsers;
            try {
                existingUsers = getUsersThatExist(terminal, env.settings(), env, options);
            } catch (Exception e) {
                throw new ElasticsearchException("failed to get users that already exist, skipping user import", e);
            }
            if (usersToMigrate.length == 0) {
                usersToMigrate = userToHashedPW.keySet().toArray(new String[userToHashedPW.size()]);
            }
            for (String user : usersToMigrate) {
                if (userToHashedPW.containsKey(user) == false) {
                    terminal.println("user [" + user + "] was not found in files, skipping");
                    continue;
                } else if (existingUsers.contains(user)) {
                    terminal.println("user [" + user + "] already exists, skipping");
                    continue;
                }
                terminal.println("migrating user [" + user + "]");
                String reqBody = "n/a";
                try {
                    reqBody = createUserJson(userToRoles.get(user), userToHashedPW.get(user));
                    String resp = postURL(env.settings(), env, "POST",
                        this.url.value(options) + "/_security/user/" + user, options, reqBody);
                    terminal.println(resp);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to migrate user [" + user + "] with body: " + reqBody, e);
                }
            }
        }

        Set<String> getRolesThatExist(Terminal terminal, Settings settings, Environment env, OptionSet options) throws Exception {
            Set<String> existingRoles = new HashSet<>();
            String allRolesJson = postURL(settings, env, "GET", this.url.value(options) + "/_security/role/", options, null);
            // EMPTY is safe here because we never use namedObject
            try (XContentParser parser = JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, allRolesJson)) {
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

        static String createRoleJson(RoleDescriptor rd) throws IOException {
            XContentBuilder builder = jsonBuilder();
            rd.toXContent(builder, ToXContent.EMPTY_PARAMS, true);
            return Strings.toString(builder);
        }

        void importRoles(Terminal terminal, Environment env, OptionSet options) throws FileNotFoundException {
            String rolesCsv = rolesToMigrateCsv.value(options);
            String[] rolesToMigrate = (rolesCsv != null) ? rolesCsv.split(",") : Strings.EMPTY_ARRAY;
            Path rolesFile = FileRolesStore.resolveFile(env).toAbsolutePath();
            if (Files.exists(rolesFile) == false) {
                throw new FileNotFoundException("roles.yml file [" + rolesFile + "] does not exist");
            }
            terminal.println("importing roles from [" + rolesFile + "]...");
            Logger logger = getTerminalLogger(terminal);
            Map<String, RoleDescriptor> roles = FileRolesStore.parseRoleDescriptors(rolesFile, logger, true, Settings.EMPTY);
            Set<String> existingRoles;
            try {
                existingRoles = getRolesThatExist(terminal, env.settings(), env, options);
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
                    reqBody = createRoleJson(roles.get(roleName));
                    String resp = postURL(env.settings(), env, "POST",
                        this.url.value(options) + "/_security/role/" + roleName, options, reqBody);
                    terminal.println(resp);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to migrate role [" + roleName + "] with body: " + reqBody, e);
                }
            }
        }
    }

    /**
     * Creates a new Logger that is detached from the ROOT logger and only has an appender that will output log messages to the terminal
     */
    static Logger getTerminalLogger(final Terminal terminal) {
        final Logger logger = LogManager.getLogger(ESNativeRealmMigrateTool.class);
        Loggers.setLevel(logger, Level.ALL);

        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();

        // create appender
        final Appender appender = new AbstractAppender(ESNativeRealmMigrateTool.class.getName(), null,
                PatternLayout.newBuilder()
                    // Specify the configuration so log4j doesn't re-initialize
                    .withConfiguration(config)
                    .withPattern("%m")
                    .build()) {
            @Override
            public void append(LogEvent event) {
                switch (event.getLevel().getStandardLevel()) {
                    case FATAL:
                    case ERROR:
                        terminal.println(Verbosity.NORMAL, event.getMessage().getFormattedMessage());
                        break;
                    case OFF:
                        break;
                    default:
                        terminal.println(Verbosity.VERBOSE, event.getMessage().getFormattedMessage());
                        break;
                }
            }
        };
        appender.start();

        // get the config, detach from parent, remove appenders, add custom appender
        final LoggerConfig loggerConfig = config.getLoggerConfig(ESNativeRealmMigrateTool.class.getName());
        loggerConfig.setParent(null);
        loggerConfig.getAppenders().forEach((s, a) -> Loggers.removeAppender(logger, a));
        Loggers.addAppender(logger, appender);
        return logger;
    }
}
