/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.SuppressForbidden;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch the Elasticsearch Windows service into the Windows Service Registry.
 */
class WindowsServiceInstallCommand extends ProcrunCommand {
    WindowsServiceInstallCommand() {
        super("Install Elasticsearch as a Windows Service", "IS");
    }

    @Override
    protected String getAdditionalArgs(String serviceId, ProcessInfo pinfo) {
        List<String> args = new ArrayList<>();
        addArg(args, "--Startup", pinfo.envVars().getOrDefault("ES_START_TYPE", "manual"));
        addArg(args, "--StopTimeout", pinfo.envVars().getOrDefault("ES_STOP_TIMEOUT", "0"));
        addArg(args, "--StartClass", "org.elasticsearch.launcher.CliToolLauncher");
        addArg(args, "--StartMethod", "main");
        addArg(args, "--StopClass", "org.elasticsearch.launcher.CliToolLauncher");
        addArg(args, "--StopMethod", "close");
        addArg(args, "--Classpath", pinfo.sysprops().get("java.class.path"));
        addArg(args, "--JvmMs", "4m");
        addArg(args, "--JvmMx", "64m");
        addQuotedArg(args, "--JvmOptions", getJvmOptions(pinfo.sysprops()));
        addArg(args, "--PidFile", "%s.pid".formatted(serviceId));
        addArg(
            args,
            "--DisplayName",
            pinfo.envVars().getOrDefault("SERVICE_DISPLAY_NAME", "Elasticsearch %s (%s)".formatted(Version.CURRENT, serviceId))
        );
        addArg(
            args,
            "--Description",
            pinfo.envVars()
                .getOrDefault("SERVICE_DESCRIPTION", "Elasticsearch %s Windows Service - https://elastic.co".formatted(Version.CURRENT))
        );
        addQuotedArg(args, "--Jvm", quote(getJvmDll(getJavaHome(pinfo.sysprops())).toString()));
        addArg(args, "--StartMode", "jvm");
        addArg(args, "--StopMode", "jvm");
        addQuotedArg(args, "--StartPath", quote(pinfo.workingDir().toString()));
        addArg(args, "++JvmOptions", "-Dcli.name=windows-service-daemon");
        addArg(args, "++JvmOptions", "-Dcli.libs=lib/tools/server-cli,lib/tools/windows-service-cli");
        addArg(args, "++Environment", "HOSTNAME=%s".formatted(pinfo.envVars().get("COMPUTERNAME")));

        String serviceUsername = pinfo.envVars().get("SERVICE_USERNAME");
        if (serviceUsername != null) {
            String servicePassword = pinfo.envVars().get("SERVICE_PASSWORD");
            assert servicePassword != null; // validated in preExecute
            addArg(args, "--ServiceUser", serviceUsername);
            addArg(args, "--ServicePassword", servicePassword);
        } else {
            addArg(args, "--ServiceUser", "LocalSystem");
        }

        String serviceParams = pinfo.envVars().get("SERVICE_PARAMS");
        if (serviceParams != null) {
            args.add(serviceParams);
        }

        return String.join(" ", args);
    }

    private static void addArg(List<String> args, String arg, String value) {
        args.add(arg);
        if (value.contains(" ")) {
            value = "\"%s\"".formatted(value);
        }
        args.add(value);
    }

    // Adds an arg with an already appropriately quoted value. Trivial, but explicit implementation.
    // This method is typically used when adding args whose value contains a file-system path
    private static void addQuotedArg(List<String> args, String arg, String value) {
        args.add(arg);
        args.add(value);
    }

    @SuppressForbidden(reason = "get java home path to pass through")
    private static Path getJavaHome(Map<String, String> sysprops) {
        return Paths.get(sysprops.get("java.home"));
    }

    private static Path getJvmDll(Path javaHome) {
        Path dll = javaHome.resolve("jre/bin/server/jvm.dll");
        if (Files.exists(dll) == false) {
            dll = javaHome.resolve("bin/server/jvm.dll");
        }
        return dll;
    }

    private static String getJvmOptions(Map<String, String> sysprops) {
        List<String> jvmOptions = new ArrayList<>();
        jvmOptions.add("-XX:+UseSerialGC");
        // passthrough these properties
        for (var prop : List.of("es.path.home", "es.path.conf", "es.distribution.type")) {
            jvmOptions.add("-D%s=%s".formatted(prop, quote(sysprops.get(prop))));
        }
        return String.join(";", jvmOptions);
    }

    @Override
    protected void preExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws UserException {
        Path javaHome = getJavaHome(pinfo.sysprops());
        terminal.println("Installing service : %s".formatted(serviceId));
        terminal.println("Using ES_JAVA_HOME : %s".formatted(javaHome.toString()));

        Path javaDll = getJvmDll(javaHome);
        if (Files.exists(javaDll) == false) {
            throw new UserException(
                ExitCodes.CONFIG,
                "Invalid java installation (no jvm.dll found in %s\\jre\\bin\\server\\ or %s\\bin\\server\"). Exiting...".formatted(
                    javaHome.toString(),
                    javaHome.toString()
                )
            );
        }

        // validate username and password come together
        boolean hasUsername = pinfo.envVars().containsKey("SERVICE_USERNAME");
        if (pinfo.envVars().containsKey("SERVICE_PASSWORD") != hasUsername) {
            throw new UserException(
                ExitCodes.CONFIG,
                "Both service username and password must be set, only got " + (hasUsername ? "SERVICE_USERNAME" : "SERVICE_PASSWORD")
            );
        }
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return "The service '%s' has been installed".formatted(serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return "Failed installing '%s' service".formatted(serviceId);
    }
}
