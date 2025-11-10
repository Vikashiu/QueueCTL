/**
 * A simple promise-based sleep function.
 * @param {number} ms - Milliseconds to sleep
 */
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Promisified version of child_process.exec
 */
import { promisify } from 'util';
import { exec as callbackExec } from 'child_process';

export const exec = promisify(callbackExec);