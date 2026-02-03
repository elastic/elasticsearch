/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import jdk.nio.Channels;
import sun.net.www.protocol.file.FileURLConnection;
import sun.net.www.protocol.jar.JarURLConnection;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse.BodySubscribers;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiPredicate;
import java.util.jar.JarFile;
import java.util.logging.FileHandler;
import java.util.zip.ZipFile;

public class FileInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(File.class)
            .calling(File::canExecute)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::canRead)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::canWrite)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::createNewFile)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::delete)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingVoid(File::deleteOnExit)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::exists)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::isDirectory)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::isFile)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::isHidden)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::lastModified)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::length)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::list)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::list, FilenameFilter.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::listFiles)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::listFiles, FileFilter.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::listFiles, FilenameFilter.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .calling(File::mkdir)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::mkdirs)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::renameTo, File.class)
            .enforce((src, dest) -> Policies.fileRead(src).and(Policies.fileWrite(dest)))
            .elseThrowNotEntitled()
            .calling(File::setExecutable, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setExecutable, Boolean.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setLastModified, Long.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setReadOnly)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setReadable, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setReadable, Boolean.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setWritable, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .calling(File::setWritable, Boolean.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(File::createTempFile, String.class, String.class)
            .enforce(Policies::createTempFile)
            .elseThrowNotEntitled()
            .callingStatic(File::createTempFile, String.class, String.class, File.class)
            .enforce((_, _, directory) -> {
                if (directory == null) {
                    return Policies.createTempFile();
                } else {
                    return Policies.fileWrite(directory);
                }
            })
            .elseThrowNotEntitled();

        builder.on(Files.class)
            .callingStatic(Files::createDirectory, Path.class, FileAttribute[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::createDirectories, Path.class, FileAttribute[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::createFile, Path.class, FileAttribute[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::createTempDirectory, Path.class, String.class, FileAttribute[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::createTempDirectory, String.class, FileAttribute[].class)
            .enforce(Policies::createTempFile)
            .elseThrowNotEntitled()
            .callingStatic(Files::createTempFile, Path.class, String.class, String.class, FileAttribute[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::createTempFile, String.class, String.class, FileAttribute[].class)
            .enforce(Policies::createTempFile)
            .elseThrowNotEntitled()
            .callingStatic(Files::copy, Path.class, Path.class, CopyOption[].class)
            .enforce(Policies.and(Policies::fileRead, Policies::fileWrite))
            .elseThrowNotEntitled()
            .callingStatic(Files::move, Path.class, Path.class, CopyOption[].class)
            .enforce(Policies.and(Policies::fileRead, Policies::fileWrite))
            .elseThrowNotEntitled()
            .callingStatic(Files::getOwner, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::probeContentType, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::setOwner, Path.class, UserPrincipal.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::newInputStream, Path.class, OpenOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::newOutputStream, Path.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::newByteChannel,
                TypeToken.of(Path.class),
                new TypeToken<Set<? extends OpenOption>>() {},
                TypeToken.of(FileAttribute[].class)
            )
            .enforce(Policies::fileReadOrWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::newByteChannel, Path.class, OpenOption[].class)
            .enforce((path, options) -> Policies.fileReadOrWrite(path, Set.of(options)))
            .elseThrowNotEntitled()
            .callingStatic(Files::newDirectoryStream, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::newDirectoryStream, Path.class, String.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::newDirectoryStream, TypeToken.of(Path.class), new TypeToken<DirectoryStream.Filter<? super Path>>() {})
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::createSymbolicLink, Path.class, Path.class, FileAttribute[].class)
            .enforce(Policies::createLink)
            .elseThrowNotEntitled()
            .callingStatic(Files::createLink, Path.class, Path.class)
            .enforce(Policies::createLink)
            .elseThrowNotEntitled()
            .callingVoidStatic(Files::delete, Path.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::deleteIfExists, Path.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::readSymbolicLink, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::getFileStore, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::isSameFile, Path.class, Path.class)
            .enforce(Policies.and(Policies::fileRead, Policies::fileRead))
            .elseThrowNotEntitled()
            .callingStatic(Files::mismatch, Path.class, Path.class)
            .enforce(Policies.and(Policies::fileRead, Policies::fileRead))
            .elseThrowNotEntitled()
            .callingStatic(Files::isHidden, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::getFileAttributeView,
                TypeToken.of(Path.class),
                new TypeToken<Class<? extends FileAttributeView>>() {},
                TypeToken.of(LinkOption[].class)
            )
            .enforce(Policies::getFileAttributeView)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::readAttributes,
                TypeToken.of(Path.class),
                new TypeToken<Class<? extends BasicFileAttributes>>() {},
                TypeToken.of(LinkOption[].class)
            )
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::setAttribute, Path.class, String.class, Object.class, LinkOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::getAttribute, Path.class, String.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::readAttributes, Path.class, String.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::getPosixFilePermissions, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::setPosixFilePermissions, TypeToken.of(Path.class), new TypeToken<Set<PosixFilePermission>>() {})
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::isSymbolicLink, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::isDirectory, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::isRegularFile, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::getLastModifiedTime, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::setLastModifiedTime, Path.class, FileTime.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::size, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::exists, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::notExists, Path.class, LinkOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::isReadable, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::isWritable, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::isExecutable, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::walkFileTree,
                TypeToken.of(Path.class),
                new TypeToken<Set<FileVisitOption>>() {},
                TypeToken.of(Integer.class),
                new TypeToken<FileVisitor<? super Path>>() {}
            )
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::walkFileTree, TypeToken.of(Path.class), new TypeToken<FileVisitor<? super Path>>() {})
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::newBufferedReader, Path.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::newBufferedReader, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::newBufferedWriter, Path.class, Charset.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::newBufferedWriter, Path.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::copy, InputStream.class, Path.class, CopyOption[].class)
            .enforce((_, target) -> Policies.fileWrite(target))
            .elseThrowNotEntitled()
            .callingStatic(Files::copy, Path.class, OutputStream.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::readAllBytes, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::readString, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::readString, Path.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::readAllLines, Path.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::readAllLines, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::write, Path.class, byte[].class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::write,
                TypeToken.of(Path.class),
                new TypeToken<Iterable<? extends CharSequence>>() {},
                TypeToken.of(Charset.class),
                TypeToken.of(OpenOption[].class)
            )
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::write,
                TypeToken.of(Path.class),
                new TypeToken<Iterable<? extends CharSequence>>() {},
                TypeToken.of(OpenOption[].class)
            )
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::writeString, Path.class, CharSequence.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::writeString, Path.class, CharSequence.class, Charset.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(Files::list, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::walk, Path.class, Integer.class, FileVisitOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::walk, Path.class, FileVisitOption[].class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(
                Files::find,
                TypeToken.of(Path.class),
                TypeToken.of(Integer.class),
                new TypeToken<BiPredicate<Path, BasicFileAttributes>>() {},
                TypeToken.of(FileVisitOption[].class)
            )
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::lines, Path.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Files::lines, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled();

        builder.on(jdk.nio.Channels.class)
            .callingStatic(jdk.nio.Channels::readWriteSelectableChannel, FileDescriptor.class, Channels.SelectableChannelCloser.class)
            .enforce(Policies::fileDescriptorWrite)
            .elseThrowNotEntitled();

        builder.on(FileChannel.class)
            .protectedCtor()
            .enforce(Policies::changeFilesHandling)
            .elseThrowNotEntitled()
            .callingStatic(FileChannel::open, Path.class, OpenOption[].class)
            .enforce((path, options) -> Policies.fileReadOrWrite(path, Set.of(options)))
            .elseThrowNotEntitled()
            .callingStatic(
                FileChannel::open,
                TypeToken.of(Path.class),
                new TypeToken<Set<? extends OpenOption>>() {},
                TypeToken.of(FileAttribute[].class)
            )
            .enforce(Policies::fileReadOrWrite)
            .elseThrowNotEntitled();

        builder.on(AsynchronousFileChannel.class)
            .protectedCtor()
            .enforce(Policies::changeFilesHandling)
            .elseThrowNotEntitled()
            .callingStatic(AsynchronousFileChannel::open, Path.class, OpenOption[].class)
            .enforce((path, options) -> Policies.fileReadOrWrite(path, Set.of(options)))
            .elseThrowNotEntitled()
            .callingStatic(
                AsynchronousFileChannel::open,
                TypeToken.of(Path.class),
                new TypeToken<Set<? extends OpenOption>>() {},
                TypeToken.of(ExecutorService.class),
                TypeToken.of(FileAttribute[].class)
            )
            .enforce(Policies::fileReadOrWrite)
            .elseThrowNotEntitled();

        builder.on(RandomAccessFile.class)
            .callingStatic(RandomAccessFile::new, File.class, String.class)
            .enforce((file, mode) -> mode.equals("r") ? Policies.fileRead(file) : Policies.fileWrite(file))
            .elseThrowNotEntitled()
            .callingStatic(RandomAccessFile::new, String.class, String.class)
            .enforce((path, mode) -> mode.equals("r") ? Policies.fileRead(new File(path)) : Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled();

        builder.on(FileInputStream.class)
            .callingStatic(FileInputStream::new, String.class)
            .enforce(path -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(FileInputStream::new, File.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(FileInputStream::new, FileDescriptor.class)
            .enforce(Policies::fileDescriptorRead)
            .elseThrowNotEntitled();

        builder.on(FileOutputStream.class)
            .callingStatic(FileOutputStream::new, String.class)
            .enforce(path -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, String.class, Boolean.class)
            .enforce((path) -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, File.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, File.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, FileDescriptor.class)
            .enforce(Policies::fileDescriptorWrite)
            .elseThrowNotEntitled();

        builder.on(FileReader.class)
            .callingStatic(FileReader::new, String.class)
            .enforce(path -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(FileReader::new, File.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(FileReader::new, File.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(FileReader::new, FileDescriptor.class)
            .enforce(Policies::fileDescriptorRead)
            .elseThrowNotEntitled()
            .callingStatic(FileReader::new, String.class, Charset.class)
            .enforce((name) -> Policies.fileRead(new File(name)))
            .elseThrowNotEntitled();

        builder.on(FileWriter.class)
            .callingStatic(FileWriter::new, String.class)
            .enforce(path -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, String.class, Boolean.class)
            .enforce((name) -> Policies.fileWrite(new File(name)))
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, File.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, File.class, Charset.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, File.class, Charset.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, File.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, FileDescriptor.class)
            .enforce(Policies::fileDescriptorWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, String.class, Charset.class)
            .enforce((name) -> Policies.fileWrite(new File(name)))
            .elseThrowNotEntitled()
            .callingStatic(FileWriter::new, String.class, Charset.class, Boolean.class)
            .enforce((name) -> Policies.fileWrite(new File(name)))
            .elseThrowNotEntitled();

        builder.on(FileInputStream.class)
            .callingStatic(FileInputStream::new, File.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(FileInputStream::new, FileDescriptor.class)
            .enforce(Policies::fileDescriptorRead)
            .elseThrowNotEntitled()
            .callingStatic(FileInputStream::new, String.class)
            .enforce(path -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled();

        builder.on(FileOutputStream.class)
            .callingStatic(FileOutputStream::new, File.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, File.class, Boolean.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, FileDescriptor.class)
            .enforce(Policies::fileDescriptorWrite)
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, String.class)
            .enforce(path -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(FileOutputStream::new, String.class, Boolean.class)
            .enforce(path -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled();

        builder.on(RandomAccessFile.class)
            .callingStatic(RandomAccessFile::new, File.class, String.class)
            .enforce((File file, String mode) -> mode.contains("w") ? Policies.fileWrite(file) : Policies.fileRead(file))
            .elseThrowNotEntitled()
            .callingStatic(RandomAccessFile::new, String.class, String.class)
            .enforce(
                (String path, String mode) -> mode.contains("w") ? Policies.fileWrite(new File(path)) : Policies.fileRead(new File(path))
            )
            .elseThrowNotEntitled();

        builder.on(JarFile.class)
            .callingStatic(JarFile::new, String.class)
            .enforce(path -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(JarFile::new, File.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(JarFile::new, String.class, Boolean.class)
            .enforce((path) -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(JarFile::new, File.class, Boolean.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(JarFile::new, File.class, Boolean.class, Integer.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(JarFile::new, File.class, Boolean.class, Integer.class, Runtime.Version.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled();

        builder.on(ZipFile.class)
            .callingStatic(ZipFile::new, String.class)
            .enforce(path -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(ZipFile::new, File.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(ZipFile::new, File.class, Integer.class)
            .enforce(Policies::fileWithZipMode)
            .elseThrowNotEntitled()
            .callingStatic(ZipFile::new, String.class, Charset.class)
            .enforce((path) -> Policies.fileRead(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(ZipFile::new, File.class, Integer.class, Charset.class)
            .enforce(Policies::fileWithZipMode)
            .elseThrowNotEntitled()
            .callingStatic(ZipFile::new, File.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled();

        builder.on(PrintWriter.class)
            .callingStatic(PrintWriter::new, File.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(PrintWriter::new, File.class, String.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(PrintWriter::new, String.class)
            .enforce(path -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled()
            .callingStatic(PrintWriter::new, String.class, String.class)
            .enforce((path) -> Policies.fileWrite(new File(path)))
            .elseThrowNotEntitled();

        builder.on(Scanner.class)
            .callingStatic(Scanner::new, File.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Scanner::new, File.class, String.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled()
            .callingStatic(Scanner::new, File.class, Charset.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled();

        builder.on(FileHandler.class)
            .callingStatic(FileHandler::new)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled()
            .callingStatic(FileHandler::new, String.class)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled()
            .callingStatic(FileHandler::new, String.class, Boolean.class)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled()
            .callingStatic(FileHandler::new, String.class, Integer.class, Integer.class)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled()
            .callingStatic(FileHandler::new, String.class, Integer.class, Integer.class, Boolean.class)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled()
            .callingStatic(FileHandler::new, String.class, Long.class, Integer.class, Boolean.class)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled()
            .callingVoid(FileHandler::close)
            .enforce(Policies::loggingFileHandler)
            .elseThrowNotEntitled();

        builder.on(BodyPublishers.class)
            .callingStatic(BodyPublishers::ofFile, Path.class)
            .enforce(Policies::fileRead)
            .elseThrowNotEntitled();

        builder.on(BodyHandlers.class)
            .callingStatic(BodyHandlers::ofFile, Path.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(BodyHandlers::ofFile, Path.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(BodyHandlers::ofFileDownload, Path.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled();

        builder.on(BodySubscribers.class)
            .callingStatic(BodySubscribers::ofFile, Path.class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled()
            .callingStatic(BodySubscribers::ofFile, Path.class, OpenOption[].class)
            .enforce(Policies::fileWrite)
            .elseThrowNotEntitled();

        builder.on(FileSystemProvider.class).protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();

        builder.on(FileURLConnection.class)
            .callingVoid(FileURLConnection::connect)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getHeaderFields)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getHeaderField, String.class)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getHeaderField, Integer.class)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getContentLength)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getContentLengthLong)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getHeaderFieldKey, Integer.class)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getLastModified)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled()
            .calling(FileURLConnection::getInputStream)
            .enforce(fuc -> Policies.urlFileRead(fuc.getURL()))
            .elseThrowNotEntitled();

        builder.on(JarURLConnection.class)
            .callingVoid(JarURLConnection::connect)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getHeaderFields)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getHeaderField, String.class)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getHeaderField, Integer.class)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getContent)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getContentLength)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getContentLengthLong)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getContentType)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getHeaderFieldKey, Integer.class)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getLastModified)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getInputStream)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getManifest)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getJarEntry)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getAttributes)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getMainAttributes)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getCertificates)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled()
            .calling(JarURLConnection::getJarFile)
            .enforce(Policies::jarURLAccess)
            .elseThrowNotEntitled();
    }
}
