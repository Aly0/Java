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
       * ������ʽ����֤�ֻ���
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
		//�����ɻ����̳߳�
		ExecutorService pool = Executors.newCachedThreadPool();
		
		//���쳣���ݴ洢��������
		List<String> falsePhone = new ArrayList<String>();
		final CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>(falsePhone);
		
		System.out.println(new Date());
		//�̳߳�
		for (int i = 0;i < 20;i++){
	           pool.execute(getThread(i,list));
	     }
		pool.shutdown();
		
		//ÿ���ȡ������
		int ftp = 0;
		int preTotal = 0;
		
		while (true) {
			//�߳�ִ����ɲ���
            if (pool.isTerminated()) {  
                //System.out.println(list);
            	//�����������������������ȷ����
                System.out.println("����������"+total + "   �쳣��������"+ falseTotal + "   ������������" + trueTotal);
                int size=list.size(); 
                //���洢�쳣���ݵĶ�̬����תΪ��ͨ���������
                String[] falsePhoneArray = (String[])list.toArray(new String[size]);
                Arrays.sort(falsePhoneArray);
                //���쳣����д���ļ�
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
            		//ÿ���ȡ����������
            		preTotal = (int)total.get();
                    Thread.sleep(1000);
                    ftp = (int)total.get() - preTotal;
                    System.out.println("��ȡ����Ϊ��"+ftp);
                    
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
		//���ļ�����20���ļ�����������
		File f = new File("G:\\task\\input");
		final List<File> filePathsList = new ArrayList<File>();
		File[] filePaths = f.listFiles();
		for(File s : filePaths) {
			filePathsList.add(s);
		}
		
		//·��ת��
		String newPath = filePathsList.get(idx).toString().replaceAll("\\\\","\\\\\\\\");
		//System.out.println(newPath);
		
		//��ȡ�ļ�
		Scanner filesIn = new Scanner(Paths.get(newPath));
		
		while(filesIn.hasNext()) {
			//���ж�ȡ
			String filesLine = filesIn.nextLine();
			//��ȡ��������
			total.incrementAndGet();
			//�����ַ��������и�
			String[] filesLineSplit = filesLine.split(",");
			//�����ж��ֻ������Ƿ���ȷ
			if(Pattern.matches(REGEX_MOBILE, filesLineSplit[0]) && Pattern.matches(REGEX_MOBILE, filesLineSplit[1])) {
				//ͳ����ȷ����
				trueTotal.incrementAndGet();
			}
			else {
				//ͳ���쳣����
				falseTotal.incrementAndGet();
				
				//���쳣������ӵ�falsePhone����
				falsePhone.add(filesLine);
			}
			//��ʱ�䵥λ��Ϊ"s"
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
			
			//��ÿ�����ݸ�ʽתΪJSONArray
			JSONArray jsonLine=JSONArray.fromObject(filesLineSplit);
			//��ӵ�����
			queue.offer(jsonLine);
			
			
		}
		
		filesIn.close();
			
	}
	
	//�Ӷ�����ȡ���ݣ���д���ļ��У�ÿ���ļ�10000��
	public static void readChangeFiles() throws FileNotFoundException {
		//�ļ��洢λ��
		String changePath = "G:\\task\\changeFiles\\changeInput-"+ k +".txt";
		if(!queue.isEmpty()) changeType = new PrintWriter(changePath);
		while(!queue.isEmpty()) {

			//ȡ����ͷ
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
				//ͳ��д����ļ�������
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
