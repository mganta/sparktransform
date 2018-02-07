package com.msft.spark;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;

public class TransformFunction {

    /** dummy read and write
     *  better if we read data to RDD
     *
     */
    public static int transform(String incomingFileName) throws IOException {

       String path = FilenameUtils.getFullPath(incomingFileName);
       String fileName = FilenameUtils.getName(incomingFileName);

       FileSystem fs = FileSystemHandler.getFileSystemHandle(path);

       FSDataInputStream inputStream = fs.open(new Path(incomingFileName));
       String fileData = IOUtils.toString(inputStream, "UTF-8");
       inputStream.close();

       FSDataOutputStream outputStream=fs.create(new Path(path + "/" + fileName + ".transformed"));
       outputStream.writeBytes(fileData);
       outputStream.close();
       return 1;
    }
}
