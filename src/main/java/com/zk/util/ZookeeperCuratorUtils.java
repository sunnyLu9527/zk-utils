package com.zk.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * zk工具类
 * Created by sunnyLu on 2017/7/12.
 */
public class ZookeeperCuratorUtils {

    /**
     *
     * @描述：创建一个zookeeper连接---连接方式一: 最简单的连接
     * @return void
     * @exception
     * @createTime：2016年5月17日
     * @author: songqinghu
     */
    public static CuratorFramework clientOne(String connectString){
        // 连接时间 和重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        //CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(connectString)
                .sessionTimeoutMs(5000).retryPolicy(retryPolicy).build();
        client.start();
        return client;
    }

    /**
     *
     * @描述：创建一个zookeeper连接---连接方式二:优选这个
     * @return void
     * @exception
     * @createTime：2016年5月17日
     * @author: songqinghu
     */
    public static CuratorFramework clientTwo(String connectString){

        //默认创建的根节点是没有做权限控制的--需要自己手动加权限???----
        ACLProvider aclProvider = new ACLProvider() {
            private List<ACL> acl ;
            @Override
            public List<ACL> getDefaultAcl() {
                if(acl ==null){
                    ArrayList<ACL> acl = ZooDefs.Ids.CREATOR_ALL_ACL;
                    acl.clear();
                    acl.add(new ACL(Perms.ALL, new Id("auth", "admin:admin") ));
                    this.acl = acl;
                }
                return acl;
            }
            @Override
            public List<ACL> getAclForPath(String path) {
                return acl;
            }
        };
        String scheme = "digest";
        byte[] auth = "admin:admin".getBytes();
        int connectionTimeoutMs = 5000;
        String namespace = "testnamespace";
        CuratorFramework client = CuratorFrameworkFactory.builder().aclProvider(aclProvider).
                authorization(scheme, auth).
                connectionTimeoutMs(connectionTimeoutMs).
                connectString(connectString).
                namespace(namespace).
                retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).build();
        client.start();
        return client;
    }

    /**
     *
     * @描述：获取子节点列表 打印
     * @return void
     * @exception
     * @createTime：2016年5月17日
     * @author: songqinghu
     * @throws Exception
     */
    public static List<String> nodesList(CuratorFramework client,String parentPath) throws Exception{
        List<String> paths = client.getChildren().forPath(parentPath);
        return paths;
    }
    /**
     *
     * @描述：创建一个节点
     * @param path
     * @return void
     * @exception
     * @createTime：2016年5月17日
     * @author: songqinghu
     * @throws Exception
     */
    public static void createNode(CuratorFramework client,String path) throws Exception{

        Stat stat = client.checkExists().forPath(path);
        System.out.println(stat);
        String forPath = client.create().creatingParentsIfNeeded().forPath(path, "create init !".getBytes());
        // String forPath = client.create().forPath(path);
        System.out.println(forPath);
    }

    /**
     * 获取指定节点中信息
     * @throws Exception
     */
    public static void getDataNode(CuratorFramework client,String path) throws Exception{
        Stat stat = client.checkExists().forPath(path);
        System.out.println(stat);
        byte[] datas = client.getData().forPath(path);
        System.out.println(new String(datas));
    }
    /**
     *
     * @描述：设置节点中的信息
     * @param client
     * @param path
     * @param message
     * @return void
     * @exception
     * @createTime：2016年5月17日
     * @author: songqinghu
     * @throws Exception
     */
    public static void setDataNode(CuratorFramework client,String path,String message) throws Exception{

        Stat stat = client.checkExists().forPath(path);
        System.out.println(stat);
        client.setData().forPath(path, message.getBytes());
    }


    public static void deleteDataNode(CuratorFramework client,String path) throws Exception{
        Stat stat = client.checkExists().forPath(path);
        if (stat == null)
            return;
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }
}
