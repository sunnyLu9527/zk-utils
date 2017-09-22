package com.zk;

import com.zk.util.ZookeeperCuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.CollectionUtils;

import java.util.List;


/**
 * 程序主入口
 * Created by sunnyLu on 2017/5/27.
 */
public class Main {
    public static void main(String[] args) {

        try {
            ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath*:/*.xml");
            String connectString = args[0];//zk链接地址
            String type = args[1];//操作类型
            String rmrNode = args[2];//用于模糊匹配的节点内容
            CuratorFramework client = ZookeeperCuratorUtils.clientOne(connectString);
            if ("brmr".equals(type)){//批量删除
                List<String> childNodeList = ZookeeperCuratorUtils.nodesList(client,"/dubbo");
                if (CollectionUtils.isEmpty(childNodeList))
                    print("没有子节点");
                for (String node : childNodeList){
                    if (node.contains(rmrNode)){
                        ZookeeperCuratorUtils.deleteDataNode(client,"/dubbo"+"/"+node);
                        print(node);
                    }
                }
                print("移除完毕");
            }
        } catch (Exception e) {
            e.printStackTrace();
            printHelp();
        }
    }


    private static void printHelp() {
        print("===============================");
        print("应用配置导入程序");
        print("Author:sunnyLu");
        print("Email:980921840@qq.com");
        print("Tel:18758040464");
        print("参数有异常，请详见REDEME.txt");
        print("祝使用愉快！！");
    }

    private static void print(String str) {
        System.out.println(str);
    }
}
