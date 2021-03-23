/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import java.util.Objects;

public class TarEntry {

    private final String name;
    private final int mode;
    private final int uid;
    private final int gid;
    private final long size;
    private final long modificationTime;
    private final Type type;
    private final String linkName;
    private final String uname;
    private final String gname;
    private final int devMajor;
    private final int devMinor;
    private final boolean ustarFormat;

    public TarEntry(String name, int mode, int uid, int gid, long size, long modificationTime, Type type, String linkName, String uname,
                    String gname, int devMajor, int devMinor, boolean ustarFormat) {
        this.name = name;
        this.mode = mode;
        this.uid = uid;
        this.gid = gid;
        this.size = size;
        this.modificationTime = modificationTime;
        this.type = type;
        this.linkName = linkName;
        this.uname = uname;
        this.gname = gname;
        this.devMajor = devMajor;
        this.devMinor = devMinor;
        this.ustarFormat = ustarFormat;
    }

    public String getName() {
        return name;
    }

    public int getMode() {
        return mode;
    }

    public int getUid() {
        return uid;
    }

    public int getGid() {
        return gid;
    }

    public long getSize() {
        return size;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public Type getType() {
        return type;
    }

    public String getLinkName() {
        return linkName;
    }

    public String getUname() {
        return uname;
    }

    public String getGname() {
        return gname;
    }

    public int getDevMajor() {
        return devMajor;
    }

    public int getDevMinor() {
        return devMinor;
    }

    public boolean isUstarFormat() {
        return ustarFormat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TarEntry tarEntry = (TarEntry) o;
        return mode == tarEntry.mode
            && uid == tarEntry.uid
            && gid == tarEntry.gid
            && size == tarEntry.size
            && modificationTime == tarEntry.modificationTime
            && devMajor == tarEntry.devMajor
            && devMinor == tarEntry.devMinor
            && ustarFormat == tarEntry.ustarFormat
            && Objects.equals(name, tarEntry.name)
            && type == tarEntry.type
            && Objects.equals(linkName, tarEntry.linkName)
            && Objects.equals(uname, tarEntry.uname)
            && Objects.equals(gname, tarEntry.gname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mode, uid, gid, size, modificationTime, type, linkName, uname, gname, devMajor, devMinor, ustarFormat);
    }

    public enum Type {
        FILE, LINK, SYMLINK, CHARACTER_DEVICE, BLOCK_DEVICE, DIRECTORY, FIFO, CONTIGUOUS_FILE;
    }

    static Type getType(String type) {
        if (type == null || type.isBlank()) {
            return Type.FILE;
        }
        return Type.values()[Integer.parseInt(type)];
    }
}
