// worker.js
const { Worker, Queue } = require('bullmq');
const fs = require('fs');
const path = require('path');
const progressFile = path.join(__dirname, 'progress.txt');
const { Redis } = require('ioredis')
// Redis 连接配置
const redisConfig = {
  host: 'localhost',
  port:  '6379',
  password: '',
  db: 0,
}
console.log(`[Redis] Connecting to Redis at ${redisConfig.host}:${redisConfig.port}`)
// 创建 Redis 连接实例
const redisClient = new Redis(redisConfig)
// Add Redis connection event listeners
redisClient.on('connect', () => {
  console.log('[Redis] Successfully connected to Redis server')
})
redisClient.on('error', (err) => {
  console.error(`[Redis] Error connecting to Redis: ${err.message}`)
})

const queue = new Queue('test', {
  connection: { host: '127.0.0.1', port: 6379 }
});

const updateProgress = async (job) => {
  for (let i = 0; i <= 10; i++) {
    const percent = i * 10;
    fs.writeFileSync(progressFile, `${percent}`);
    await job.updateProgress(percent);
    console.log(`第${i}s 进度 ${percent}`);
    await new Promise(res => setTimeout(res, 1000));
  }
}

const worker = new Worker('test', async job => {
  try {
    if (job.name === 'convert') {
      // 1. 提交转换申请，模拟写进度到 progress.txt
      console.log('提交转换申请:', job.data);
      const jobId = job.data.jobId;
      queue.add('progress', {
        filename: job.data.filename,
        jobId: job.data.jobId,
        attempt: 1
      });

      await redisClient.hmset(`test:file:${jobId}`, {
        progress: 0,
        status: 'processing',
        updatedAt: Date.now()
      });
      updateProgress(job);
      return { status: 'submitted' };
    }
    if (job.name === 'progress') {
      // 2. 查询转换进度，轮询 progress.txt
      const jobId = job.data.jobId;
      let percent = 0;
      try {
        // 模拟从服务端读取进度信息
        percent = parseInt(fs.readFileSync(progressFile, 'utf8'));
      } catch (e) {
        percent = 0;
      }
      await redisClient.hmset(`test:file:${jobId}`, {
        progress: percent,
        status: 'processing',
        updatedAt: Date.now()
      });
      const redisStatus = await redisClient.hgetall(`test:file:${jobId}`);
      console.log('查询转换进度:', percent, redisStatus);
      if (percent >= 100) {
        // 进度100，触发 callback
        await queue.add('callback', {
          filename: job.data.filename,
          jobId: job.data.jobId
        });
        return { progress: 100 };
      } else {
        // 指数退避添加下一个 progress 任务
        const attempt = job.data.attempt || 1;
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 30000); // 最大30秒
        console.log(`下次检查延迟: ${delay}ms`);
        await queue.add('progress', {
          filename: job.data.filename,
          jobId: job.data.jobId,
          attempt: attempt + 1
        }, { delay });
        return { progress: percent, nextPollInMs: delay };
      }
    }
    if (job.name === 'callback') {
      // 3. 完成回调
      const jobId = job.data.jobId;

      const file = job.data.filename.replace('.docx', '_converted.pdf');
      console.log('完成回调: 转换完成, 文件:', file);
      await job.updateProgress(100);
      await redisClient.hmset(`test:file:${jobId}`, {
        progress: 100,
        status: 'done',
        updatedAt: Date.now()
      });
      return { result: 'success', file };
    }
  } catch (error) {
    console.error(`任务处理错误: ${error.message}`);
    throw error;
  }
}, {
  connection: { host: '127.0.0.1', port: 6379 }
});

// 添加优雅关闭
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

async function shutdown() {
  console.log('正在关闭服务...');
  await worker.close();
  await queue.close();
  await redisClient.quit();
  console.log('服务已关闭');
  process.exit(0);
}
