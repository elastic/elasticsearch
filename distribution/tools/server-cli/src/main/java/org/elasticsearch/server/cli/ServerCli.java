/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.PathConverter;

import org.elasticsearch.Build;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.server.launcher.common.LaunchDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeSet;

/**
 * The main CLI for preparing the Elasticsearch server launch.
 *
 * <p> This program (the "preparer") does all the heavy lifting: parsing options, loading secure
 * settings, auto-configuring security, syncing plugins, computing JVM options, and building the
 * full command line. It then writes a {@link LaunchDescriptor} to stdout and exits. The
 * server-launcher runs the preparer with stdout redirected and reads the descriptor from the
 * preparer's stdout pipe, then spawns the actual server process.
 */
class ServerCli extends EnvironmentAwareCommand {

    /**
     * The inclusive range of values glibc accepts for one malloc environment variable.
     */
    record GlibcEnvLimits(long min, long max) {
        /**
         * @return {@code value} parsed as a decimal number, or empty if it is not one or is out of range
         */
        OptionalLong parse(String value) {
            try {
                long parsed = Long.parseLong(value);
                return parsed >= min && parsed <= max ? OptionalLong.of(parsed) : OptionalLong.empty();
            } catch (NumberFormatException e) {
                return OptionalLong.empty();
            }
        }
    }

    /**
     * Prefix of the system properties that are passed to the server process as environment variables:
     * {@code -Des.glibc.env.MALLOC_ARENA_MAX=96} sets {@code MALLOC_ARENA_MAX=96}.
     *
     * <p> glibc reads its malloc parameters ({@code MALLOC_ARENA_MAX}, {@code MALLOC_MMAP_THRESHOLD_},
     * {@code MALLOC_TRIM_THRESHOLD_}, ...) from the environment when the process starts, so they cannot be changed from inside
     * the server once it runs. Passing them as system properties lets this launcher set them on the server process environment.
     * Only the names in {@link #GLIBC_MALLOC_ENV_VARS} are accepted.
     */
    static final String GLIBC_ENV_SYSPROP_PREFIX = "es.glibc.env.";

    private static final String MALLOC_MMAP_THRESHOLD = "MALLOC_MMAP_THRESHOLD_";
    private static final String MALLOC_TRIM_THRESHOLD = "MALLOC_TRIM_THRESHOLD_";

    /**
     * The glibc malloc environment variables that can be set through {@link #GLIBC_ENV_SYSPROP_PREFIX}, with the values glibc
     * accepts for each. glibc silently ignores an unknown variable or a value out of range, which would leave the process with
     * the default behaviour while looking configured, so we reject both instead.
     */
    static final Map<String, GlibcEnvLimits> GLIBC_MALLOC_ENV_VARS = Map.of(
        "MALLOC_ARENA_MAX",
        new GlibcEnvLimits(1, Long.MAX_VALUE),
        "MALLOC_ARENA_TEST",
        new GlibcEnvLimits(1, Long.MAX_VALUE),
        MALLOC_MMAP_THRESHOLD,
        new GlibcEnvLimits(0, 32L * 1024 * 1024),
        MALLOC_TRIM_THRESHOLD,
        new GlibcEnvLimits(0, Long.MAX_VALUE),
        "MALLOC_TOP_PAD_",
        new GlibcEnvLimits(0, Long.MAX_VALUE),
        "MALLOC_MMAP_MAX_",
        new GlibcEnvLimits(0, Integer.MAX_VALUE)
    );

    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;
    private final OptionSpec<String> enrollmentTokenOption;

    // visible for testing
    ServerCli() {
        super("Starts Elasticsearch"); // we configure logging later, so we override the base class from configuring logging
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"), "Prints Elasticsearch version information and exits");
        daemonizeOption = parser.acceptsAll(Arrays.asList("d", "daemonize"), "Starts Elasticsearch in the background")
            .availableUnless(versionOption);
        pidfileOption = parser.acceptsAll(Arrays.asList("p", "pidfile"), "Creates a pid file in the specified path on start")
            .availableUnless(versionOption)
            .withRequiredArg()
            .withValuesConvertedBy(new PathConverter());
        quietOption = parser.acceptsAll(Arrays.asList("q", "quiet"), "Turns off standard output/error streams logging in console")
            .availableUnless(versionOption)
            .availableUnless(daemonizeOption);
        enrollmentTokenOption = parser.accepts("enrollment-token", "An existing enrollment token for securely joining a cluster")
            .availableUnless(versionOption)
            .withRequiredArg();
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            printVersion(terminal);
            return;
        }

        validateConfig(options, env);

        var secureSettingsLoader = secureSettingsLoader(processInfo);

        try (
            var loadedSecrets = secureSettingsLoader.load(env, terminal);
            var password = (loadedSecrets.password().isPresent()) ? loadedSecrets.password().get() : new SecureString(new char[0]);
        ) {
            SecureSettings secrets = loadedSecrets.secrets();
            if (secureSettingsLoader.supportsSecurityAutoConfiguration()) {
                env = autoConfigureSecurity(terminal, options, processInfo, env, password);
                // reload or create the secrets
                secrets = secureSettingsLoader.bootstrap(env, password);
            }

            // we should have a loaded or bootstrapped secure settings at this point
            if (secrets == null) {
                throw new UserException(ExitCodes.CONFIG, "Elasticsearch secure settings not configured");
            }

            // install/remove plugins from elasticsearch-plugins.yml
            syncPlugins(terminal, env, processInfo);

            ServerArgs args = createArgs(options, env, secrets, processInfo);
            prepareLaunch(terminal, processInfo, args, options.has(daemonizeOption));
        }
    }

    private static void printVersion(Terminal terminal) {
        final String versionOutput = String.format(
            Locale.ROOT,
            "Version: %s, Build: %s/%s/%s, JVM: %s",
            Build.current().qualifiedVersion(),
            Build.current().type().displayName(),
            Build.current().hash(),
            Build.current().date(),
            JvmInfo.jvmInfo().version()
        );
        terminal.println(versionOutput);
    }

    private void validateConfig(OptionSet options, Environment env) throws UserException {
        if (options.valuesOf(enrollmentTokenOption).size() > 1) {
            throw new UserException(ExitCodes.USAGE, "Multiple --enrollment-token parameters are not allowed");
        }

        Path log4jConfig = env.configDir().resolve("log4j2.properties");
        if (Files.exists(log4jConfig) == false) {
            throw new UserException(ExitCodes.CONFIG, "Missing logging config file at " + log4jConfig);
        }
    }

    // Autoconfiguration of SecureSettings is currently only supported for KeyStore based secure settings
    // package private for testing
    Environment autoConfigureSecurity(
        Terminal terminal,
        OptionSet options,
        ProcessInfo processInfo,
        Environment env,
        SecureString keystorePassword
    ) throws Exception {
        assert secureSettingsLoader(processInfo) instanceof KeyStoreLoader;

        String autoConfigLibs = "modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli";
        Command cmd = loadTool(processInfo.sysprops(), "auto-configure-node", autoConfigLibs);
        assert cmd instanceof EnvironmentAwareCommand;
        @SuppressWarnings("raw")
        var autoConfigNode = (EnvironmentAwareCommand) cmd;
        final String[] autoConfigArgs;
        if (options.has(enrollmentTokenOption)) {
            autoConfigArgs = new String[] { "--enrollment-token", options.valueOf(enrollmentTokenOption) };
        } else {
            autoConfigArgs = new String[0];
        }
        OptionSet autoConfigOptions = autoConfigNode.parseOptions(autoConfigArgs);

        boolean changed = true;
        try (var autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword.clone())) {
            autoConfigNode.execute(autoConfigTerminal, autoConfigOptions, env, processInfo);
        } catch (UserException e) {
            boolean okCode = switch (e.exitCode) {
                // these exit codes cover the cases where auto-conf cannot run but the node should NOT be prevented from starting as usual
                // e.g. the node is restarted, is already configured in an incompatible way, or the file system permissions do not allow it
                case ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP -> true;
                default -> false;
            };
            if (options.has(enrollmentTokenOption) == false && okCode) {
                // we still want to print the error, just don't fail startup
                if (e.getMessage() != null) {
                    terminal.errorPrintln(e.getMessage());
                }
                changed = false;
            } else {
                throw e;
            }
        }
        if (changed) {
            // reload settings since auto security changed them
            env = createEnv(options, processInfo);
        }
        return env;
    }

    // package private for testing
    void syncPlugins(Terminal terminal, Environment env, ProcessInfo processInfo) throws Exception {
        String pluginCliLibs = "lib/tools/plugin-cli";
        Command cmd = loadTool(processInfo.sysprops(), "sync-plugins", pluginCliLibs);
        assert cmd instanceof EnvironmentAwareCommand;
        @SuppressWarnings("raw")
        var syncPlugins = (EnvironmentAwareCommand) cmd;
        syncPlugins.execute(terminal, syncPlugins.parseOptions(new String[0]), env, processInfo);
    }

    private static void validatePidFile(Path pidFile) throws UserException {
        Path parent = pidFile.getParent();
        if (parent != null && Files.exists(parent) && Files.isDirectory(parent) == false) {
            throw new UserException(ExitCodes.USAGE, "pid file parent [" + parent + "] exists but is not a directory");
        }
        if (Files.exists(pidFile) && Files.isRegularFile(pidFile) == false) {
            throw new UserException(ExitCodes.USAGE, pidFile + " exists but is not a regular file");
        }
    }

    private ServerArgs createArgs(OptionSet options, Environment env, SecureSettings secrets, ProcessInfo processInfo)
        throws UserException {
        boolean daemonize = options.has(daemonizeOption);
        boolean quiet = options.has(quietOption);
        Path pidFile = null;
        if (options.has(pidfileOption)) {
            pidFile = options.valueOf(pidfileOption);
            if (pidFile.isAbsolute() == false) {
                pidFile = processInfo.workingDir().resolve(pidFile.toString()).toAbsolutePath();
            }
            validatePidFile(pidFile);
        }
        return new ServerArgs(daemonize, quiet, pidFile, secrets, env.settings(), env.configDir(), env.logsDir());
    }

    // protected to allow tests to override
    protected Command loadTool(Map<String, String> sysprops, String toolname, String libs) {
        return CliToolProvider.load(sysprops, toolname, libs).create();
    }

    /**
     * Builds a {@link LaunchDescriptor} with all the information the launcher needs to spawn
     * the server process and writes it to the terminal's output stream (stdout when the
     * launcher runs the preparer with redirect mode).
     */
    // protected to allow tests to override
    protected void prepareLaunch(Terminal terminal, ProcessInfo processInfo, ServerArgs args, boolean daemonize) throws Exception {
        var tempDir = ServerProcessUtils.setupTempDir(processInfo);
        var jvmOptions = JvmOptionsParser.determineJvmOptions(args, processInfo, tempDir, new MachineDependentHeap());

        String command = getJavaCommand(processInfo);
        List<String> jvmArgs = getJvmArgs(processInfo);
        Map<String, String> environment = getEnvironment(processInfo, tempDir);
        addGlibcEnvironment(terminal, jvmOptions, environment);
        byte[] serverArgsBytes = serializeServerArgs(args);

        LaunchDescriptor descriptor = new LaunchDescriptor(
            command,
            jvmOptions,
            jvmArgs,
            environment,
            args.logsDir().toString(),
            tempDir.toString(),
            daemonize,
            serverArgsBytes
        );

        var out = terminal.getOutputStream();
        if (out == null) {
            throw new IllegalStateException("terminal.getOutputStream() is null; preparer must be run with ES_REDIRECT_STDOUT_TO_STDERR");
        }
        DataOutputStream dos = new DataOutputStream(out);
        descriptor.writeTo(dos);
        dos.flush();
    }

    protected static String getJavaCommand(ProcessInfo processInfo) {
        Path javaHome = Path.of(processInfo.sysprops().get("java.home"));
        boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
        return javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString();
    }

    protected static List<String> getJvmArgs(ProcessInfo processInfo) {
        Path esHome = processInfo.workingDir();
        return List.of(
            "--module-path",
            esHome.resolve("lib").toString(),
            "--add-modules=jdk.net",
            "--add-modules=jdk.management.agent",
            "--add-modules=ALL-MODULE-PATH",
            "-m",
            "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch"
        );
    }

    protected static Map<String, String> getEnvironment(ProcessInfo processInfo, Path tempDir) {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());
        envVars.remove("ES_TMPDIR");
        if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
            envVars.put("LIBFFI_TMPDIR", tempDir.toString());
        }
        envVars.remove("ES_JAVA_OPTS");
        return envVars;
    }

    /**
     * Adds an environment variable to {@code environment} for each {@link #GLIBC_ENV_SYSPROP_PREFIX} system property in
     * {@code jvmOptions}, overriding any variable of the same name. Prints each one so that the log shows what was set.
     *
     * @throws UserException if a variable is not in {@link #GLIBC_MALLOC_ENV_VARS}, or its value is not a decimal number in
     *                       the accepted range
     */
    static void addGlibcEnvironment(Terminal terminal, List<String> jvmOptions, Map<String, String> environment) throws UserException {
        boolean thresholdSet = false;
        for (Map.Entry<String, String> property : JvmErgonomics.extractSystemProperties(jvmOptions).entrySet()) {
            String key = property.getKey();
            if (key.startsWith(GLIBC_ENV_SYSPROP_PREFIX) == false) {
                continue;
            }
            String name = key.substring(GLIBC_ENV_SYSPROP_PREFIX.length());
            String value = parseGlibcEnvironmentValue(key, name, property.getValue());
            environment.put(name, value);
            thresholdSet |= name.equals(MALLOC_MMAP_THRESHOLD) || name.equals(MALLOC_TRIM_THRESHOLD);
            terminal.println("Setting environment variable [" + name + "=" + value + "] from system property [" + key + "]");
        }
        // Setting either threshold turns off glibc's dynamic adjustment of both; the one left unset stays at its default.
        if (thresholdSet && environment.containsKey(MALLOC_MMAP_THRESHOLD) != environment.containsKey(MALLOC_TRIM_THRESHOLD)) {
            String unset = environment.containsKey(MALLOC_MMAP_THRESHOLD) ? MALLOC_TRIM_THRESHOLD : MALLOC_MMAP_THRESHOLD;
            terminal.errorPrintln(
                "Warning: setting only one of ["
                    + MALLOC_MMAP_THRESHOLD
                    + "] and ["
                    + MALLOC_TRIM_THRESHOLD
                    + "] fixes both, and ["
                    + unset
                    + "] stays at its glibc default; set both explicitly"
            );
        }
    }

    /**
     * Process one glibc environment variable, validates it and returns its value in canonical
     * decimal form. The canonical form matters because glibc parses tunable values differently from
     * Java, as C literals. Passing the original string could apply a different value from the
     * one we validated.
     */
    private static String parseGlibcEnvironmentValue(String key, String name, String value) throws UserException {
        GlibcEnvLimits limits = GLIBC_MALLOC_ENV_VARS.get(name);
        if (limits == null) {
            throw new UserException(
                ExitCodes.CONFIG,
                "System property ["
                    + key
                    + "] sets unsupported environment variable ["
                    + name
                    + "]; supported variables are "
                    + new TreeSet<>(GLIBC_MALLOC_ENV_VARS.keySet())
            );
        }

        OptionalLong parsed = limits.parse(value);
        if (parsed.isEmpty()) {
            throw new UserException(
                ExitCodes.CONFIG,
                "System property [" + key + "] must be a decimal number between [" + limits.min() + "] and [" + limits.max() + "]"
            );
        }
        return Long.toString(parsed.getAsLong());
    }

    protected static byte[] serializeServerArgs(ServerArgs args) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            args.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    // protected to allow tests to override
    protected SecureSettingsLoader secureSettingsLoader(ProcessInfo processInfo) {
        // The SecureSettingsLoader is configured by a CLI sys prop `es.secure_settings.source` via `CLI_JAVA_OPTS`
        String source = processInfo.sysprops().getOrDefault("es.secure_settings.source", "keystore");
        return switch (source) {
            case "keystore" -> new KeyStoreLoader();
            case "file_settings" -> new FileSettingsClusterSecretsLoader();
            default -> throw new IllegalArgumentException("Unknown secure settings source [" + source + "]");
        };
    }
}
