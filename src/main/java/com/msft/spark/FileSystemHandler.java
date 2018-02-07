package com.msft.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class FileSystemHandler {
    private static FileSystem fs;

    static FileSystem getFileSystemHandle(String ioPath) throws IOException {
        if(fs == null)
            fs = FileSystem.get(URI.create(ioPath), new Configuration());
        return fs;
    }
}
