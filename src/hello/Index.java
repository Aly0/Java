package hello;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.io.*;
import java.nio.file.Paths;
import net.sf.json.*;



public class Index {
	/**
       * 正则表达式：验证手机号
    */
    public static final String REGEX_MOBILE = "^((17[0-9])|(14[0-9])|(13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$";
	public static ConcurrentLinkedQueue<JSONArray> queue = new ConcurrentLinkedQueue<JSONArray>();
	public static PrintWriter changeType;
	public static AtomicInteger changeNum = new AtomicInteger();
	public static AtomicInteger total = new AtomicInteger();
	public static AtomicInteger falseTotal = new AtomicInteger();
	public static AtomicInteger trueTotal = new AtomicInteger();
	public static int k = 0;
	public static int count = 0;
	
	
	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException
	{
		PrintWriter out = new PrintWriter("G:\\task\\ffile.txt");
		//创建可缓存线程池
		ExecutorService pool = Executors.newCachedThreadPool();
		
		//将异常数据存储到数组中
		List<String> falsePhone = new ArrayList<String>();
		final CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>(falsePhone);
		
		System.out.println(new Date());
		//线程池
		for (int i = 0;i < 20;i++){
	           pool.execute(getThread(i,list));
	     }
		pool.shutdown();
		
		//每秒读取数据量
		int ftp = 0;
		int preTotal = 0;
		
		while (true) {
			//线程执行完成操作
            if (pool.isTerminated()) {  
                //System.out.println(list);
            	//输出总条数，错误条数，正确条数
                System.out.println("总数据量："+total + "   异常数据量："+ falseTotal + "   正常数据量：" + trueTotal);
                int size=list.size(); 
                //将存储异常数据的动态数组转为普通数组后排序
                String[] falsePhoneArray = (String[])list.toArray(new String[size]);
                Arrays.sort(falsePhoneArray);
                //将异常数据写入文件
                for(int i = 0;i < falsePhoneArray.length;i++) {
                	out.write(falsePhoneArray[i] + "\r\n");
                }
                out.close();
                //System.out.println(queue.size());
               
                readChangeFiles();
                System.out.println(new Date());
                break;  
            }  
            else {
            	try {
            		//每秒读取数据量计算
            		preTotal = (int)total.get();
                    Thread.sleep(1000);
                    ftp = (int)total.get() - preTotal;
                    System.out.println("读取条数为："+ftp);
                    
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }  

	}
	private static Runnable getThread(int i,CopyOnWriteArrayList<String> falsePhone){
        return new Runnable() {
            public void run() {
         	
                try {
                	deal(i,falsePhone);
                }catch (Exception e){

                }
                
            }
        };
    }
	
	public static void deal(int idx,CopyOnWriteArrayList<String> falsePhone) throws IOException,FileNotFoundException, InterruptedException
	{	
		//将文件夹中20个文件放在数组中
		File f = new File("G:\\task\\input");
		final List<File> filePathsList = new ArrayList<File>();
		File[] filePaths = f.listFiles();
		for(File s : filePaths) {
			filePathsList.add(s);
		}
		
		//路径转换
		String newPath = filePathsList.get(idx).toString().replaceAll("\\\\","\\\\\\\\");
		//System.out.println(newPath);
		
		//读取文件
		Scanner filesIn = new Scanner(Paths.get(newPath));
		
		while(filesIn.hasNext()) {
			//按行读取
			String filesLine = filesIn.nextLine();
			//读取数据总数
			total.incrementAndGet();
			//单行字符串数据切割
			String[] filesLineSplit = filesLine.split(",");
			//正则判断手机号码是否正确
			if(Pattern.matches(REGEX_MOBILE, filesLineSplit[0]) && Pattern.matches(REGEX_MOBILE, filesLineSplit[1])) {
				//统计正确条数
				trueTotal.incrementAndGet();
			}
			else {
				//统计异常条数
				falseTotal.incrementAndGet();
				
				//将异常数据添加到falsePhone数组
				falsePhone.add(filesLine);
			}
			//将时间单位换为"s"
			char timeUnit = filesLineSplit[2].charAt(filesLineSplit[2].length()-1);
			int timeNum = Integer.valueOf(filesLineSplit[2].substring(0, filesLineSplit[2].length()-1)).intValue();
			if(timeUnit == 'm') {
				filesLineSplit[2] = "" + timeNum * 60 + "s";
			}
			else if(timeUnit == 'h') {
				filesLineSplit[2] = "" + timeNum * 60 * 60 + "s";
			}
			else {
				filesLineSplit[2] = filesLineSplit[2];
			}
			
			//将每条数据格式转为JSONArray
			JSONArray jsonLine=JSONArray.fromObject(filesLineSplit);
			//添加到队列
			queue.offer(jsonLine);
			
			
		}
		
		filesIn.close();
			
	}
	
	//从队列中取数据，并写到文件中，每个文件10000条
	public static void readChangeFiles() throws FileNotFoundException {
		//文件存储位置
		String changePath = "G:\\task\\changeFiles\\changeInput-"+ k +".txt";
		if(!queue.isEmpty()) changeType = new PrintWriter(changePath);
		while(!queue.isEmpty()) {

			//取队列头
			changeType.write(queue.poll()+"\r\n");
			count++;
			if(count == 10000) {
				count = 0;
				k++;
				readChangeFiles();
			}
			
			/*Scanner readChange = new Scanner(changePath);
			while(readChange.hasNext()) {
				readChange.nextLine();
				//统计写入的文件的条数
				changeNum.incrementAndGet();
				if(changeNum.get() == 10000) {
					k++;
					//readChange.close();
					changeNum.getAndSet(0);
					readChangeFiles();
				}
			}
			readChange.close();
			*/
		}
		changeType.close();
	}
	
	
}
