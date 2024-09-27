from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr
from pymongo import MongoClient
from bson import ObjectId
from typing import List, Optional
from datetime import datetime



# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client.job_board  
jobs_collection = db.jobs  
applications_collection = db.applications  

# Pydantic Models
class ObjectIdModel(BaseModel):
    __root__: str

    class Config:
        arbitrary_types_allowed = True

class JobPosting(BaseModel):
   
    
    title: str
    description: str
    company_name: str
    location: str
    posted_date: datetime
    salary: Optional[str] = None
    tags: Optional[List[str]] = None


class JobApplication(BaseModel):
   
    job_id: ObjectIdModel
    applicant_name: str
    email: EmailStr
    resume: str
    application_date: datetime


app = FastAPI()

def job_to_dict(job):
    job_dict = job.copy()
    job_dict["id"] = str(job_dict["_id"])  
    del job_dict["_id"] 
    return job_dict

def application_to_dict(application):
    app_dict = application.copy()
    app_dict["id"] = str(app_dict["_id"])  
    app_dict["job_id"] = str(app_dict["job_id"])
    del app_dict["_id"]  
    return app_dict


@app.post("/jobs/create", response_model=JobPosting)
async def create_job(job: JobPosting):
    job_dict = job.dict()
    job_dict["_id"] = ObjectId()  
    jobs_collection.insert_one(job_dict)
    return job_to_dict(job_dict)  

@app.get("/jobs/get", response_model=List[JobPosting])
async def get_jobs(
    location: Optional[str] = Query(None, description="Filter jobs by location"),
    company_name: Optional[str] = Query(None, description="Filter jobs by company name"),
    tags: Optional[List[str]] = Query(None, description="Filter jobs by tags/skills as an array")
):
    query = {}
    if location:
        query["location"] = location
    if company_name:
        query["company_name"] = company_name
    if tags:
        query["tags"] = tags 
    
    jobs = [job_to_dict(job) for job in jobs_collection.find(query)]
    return jobs

@app.get("/jobs/grt/{id}", response_model=JobPosting)
async def get_job(id: str):
    job = jobs_collection.find_one()
    if job:
        return job_to_dict(job)
    raise HTTPException(status_code=404, detail="Job not found")

@app.put("/jobs/update/{id}", response_model=JobPosting)
async def update_job(id: str, job: JobPosting):
    result = jobs_collection.update_one({"_id": ObjectId(id)},{"$set": job.dict()})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.delete("/jobs/delete/{id}")
async def delete_job(id: str):
    result = jobs_collection.delete_one({"_id": ObjectId(id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"detail": "Job deleted"}


@app.post("/application/create/", response_model=JobApplication)
async def create_application(application: JobApplication):
    application_dict = application.dict()
    #application_dict["job_id"] = ObjectId(application.job_id.__root__)  
    applications_collection.insert_one(application_dict)
    return application_to_dict(application_dict)  
@app.get("/application/get/{id}", response_model=JobApplication)
async def get_application(id:str):
    application = applications_collection.find_one({"_id": ObjectId(id)})
    if application:
        return application_to_dict(application)
    raise HTTPException(status_code=404, detail="Application not found")
@app.get("/jobs/{job_id}/applications")
async def get_applications_for_job(job_id: str):
   
    query = {"job_id": job_id}  
    application_count = applications_collection.count_documents(query)

    applications = [application_to_dict(app) for app in applications_collection.find(query)]

    print(f"Found {application_count} applications for Job ID: {job_id}")
    print(f"Applications: {applications}")

    return {
        "job_id": job_id,
        "application_count": application_count,
        "applications": applications
    }



    