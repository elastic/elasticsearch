/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.GeneralSecurityException;
import java.security.UnrecoverableKeyException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for common file handling in SSL configuration
 */
final class SslFileUtil {

    static String pathsToString(List<Path> paths) {
        return paths.stream().map(Path::toAbsolutePath).map(Object::toString).collect(Collectors.joining(","));
    }

    static SslConfigException ioException(String fileType, List<Path> paths, IOException cause) {
        return ioException(fileType, paths, cause, null);
    }

    static SslConfigException ioException(String fileType, List<Path> paths, IOException cause, String detail) {
        if (cause instanceof FileNotFoundException || cause instanceof NoSuchFileException) {
            return fileNotFound(fileType, paths, cause);
        }
        if (cause instanceof AccessDeniedException) {
            return accessDenied(fileType, paths, (AccessDeniedException) cause);
        }
        String message = "cannot read configured " + fileType;
        if (paths.isEmpty() == false) {
            message += " [" + pathsToString(paths) + "]";
        }

        if (hasCause(UnrecoverableKeyException.class, cause)) {
            message += " - this is usually caused by an incorrect password";
        } else if (cause != null && cause.getMessage() != null) {
            message += " - " + cause.getMessage();
        }

        if (detail != null) {
            message = message + "; " + detail;
        }
        return new SslConfigException(message, cause);
    }

    static SslConfigException fileNotFound(String fileType, List<Path> paths, IOException cause) {
        String message = "cannot read configured " + fileType + " [" + pathsToString(paths) + "] because ";
        if (paths.size() == 1) {
            message += "the file does not exist";
        } else {
            message += "one or more files do not exist";
        }
        return new SslConfigException(message, cause);
    }

    static SslConfigException accessDenied(String fileType, List<Path> paths, AccessDeniedException cause) {
        String message = "not permitted to read ";
        if (paths.size() == 1) {
            message += "the " + fileType + " file";
        } else {
            message += "one of more " + fileType + "files";
        }
        message += " [" + pathsToString(paths) + "]";
        return new SslConfigException(message, cause);
    }

    static SslConfigException accessControlFailure(String fileType, List<Path> paths, AccessControlException cause, Path basePath) {
        String message = "cannot read configured " + fileType + " [" + pathsToString(paths) + "] because ";
        if (paths.size() == 1) {
            message += "access to read the file is blocked";
        } else {
            message += "access to read one or more files is blocked";
        }
        message += "; SSL resources should be placed in the ";
        if (basePath == null) {
            message += "Elasticsearch config directory";
        } else {
            message += "[" + basePath + "] directory";
        }
        return new SslConfigException(message, cause);
    }

    public static SslConfigException securityException(String fileType, List<Path> paths, GeneralSecurityException cause) {
        return securityException(fileType, paths, cause, null);
    }

    public static SslConfigException securityException(String fileType, List<Path> paths, GeneralSecurityException cause, String detail) {
        String message = "cannot load " + fileType;
        if (paths.isEmpty() == false) {
            message += " from [" + pathsToString(paths) + "]";
        }
        message += " due to " + cause.getClass().getSimpleName();
        if (cause.getMessage() != null) {
            message += " (" + cause.getMessage() + ")";
        }
        if (detail != null) {
            message = message + "; " + detail;
        } else if (hasCause(UnrecoverableKeyException.class, cause)) {
            message += "; this is usually caused by an incorrect password";
        }

        return new SslConfigException(message, cause);
    }

    private static boolean hasCause(Class<? extends Throwable> exceptionType, Throwable exception) {
        if (exception == null) {
            return false;
        }
        if (exceptionType.isInstance(exception)) {
            return true;
        }

        final Throwable cause = exception.getCause();
        if (cause == null || cause == exception) {
            return false;
        }
        return hasCause(exceptionType, cause);
    }

    public static SslConfigException configException(String fileType, List<Path> paths, SslConfigException cause) {
        String message = "cannot load " + fileType;
        if (paths.isEmpty() == false) {
            message += " from [" + pathsToString(paths) + "]";
        }
        message += " - " + cause.getMessage();
        return new SslConfigException(message, cause);
    }
}
