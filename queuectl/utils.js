
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

import { promisify } from 'util';
import { exec as callbackExec } from 'child_process';

export const exec = promisify(callbackExec);