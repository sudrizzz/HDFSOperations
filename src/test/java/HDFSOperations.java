import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HDFSOperations {
    @Test
    public void readFile() throws Exception {
        String uri = "hdfs://master:9000/user/hadoop/test";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        InputStream in = null;
        try {
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Test
    public void writeFile() throws Exception {
        String source = "C:\\Users\\Desktop\\test";
        String destination = "hdfs://master:9000/user/hadoop/test2";
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(source));
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(destination), configuration);
        OutputStream outputStream = fileSystem.create(new Path(destination));
        IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }

    @Test
    public void createFolder() throws Exception {
        String uri = "hdfs://master:9000/user/hadoop";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);
        fileSystem.mkdirs(path);
        fileSystem.close();
    }

    @Test
    public void deleteFile() throws Exception {
        String uri = "hdfs://master:9000/user/hadoop/test2";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path("hdfs://master:9000/user/hadoop");
        boolean isDeleted = fileSystem.delete(path, true);
        System.out.println(isDeleted);
        fileSystem.close();
    }

    @Test
    public void listFiles() throws Exception {
        String uri = "hdfs://master:9000/user";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);
        FileStatus[] status = fileSystem.listStatus(path);
        for (FileStatus fileStatus : status) {
            System.out.println(fileStatus.getPath().toString());
        }
        fileSystem.close();
    }
}
